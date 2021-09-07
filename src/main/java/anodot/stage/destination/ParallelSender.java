package anodot.stage.destination;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.service.dataformats.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.Errors;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;

public class ParallelSender {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelSender.class);

    private final BlockingQueue<Future<BatchResponse>> completionQueue = new LinkedBlockingQueue<>();
    private final String targetIdentifier;//Helps us to track the instance through multithreaded environment
    private final String propertiesFieldPath;
    private final String partitionFieldKeyPath;
    private final long maxSendResponseWait;
    private final int parallelSendersNumber;
    private static final int DEFAULT_PARTITION = 0;
    private final Worker[] workersPool;
    /**
     * We do not rely on completionQueue.size() since there is a short race between the moment the send() method returns and the future is actually added to the completionQueue on sending thread
     **/
    private       long pendingTasks = 0;
    public ParallelSender(AnodotTargetConfig conf,
                          Target.Context context,
                          List<Stage.ConfigIssue> issues, String targetIdentifier) {
        parallelSendersNumber = conf.parallelSendersNumber;
        this.targetIdentifier = targetIdentifier;
        workersPool = new Worker[parallelSendersNumber];
        propertiesFieldPath = conf.propertiesPath;
        partitionFieldKeyPath = conf.partitioningKeyPath;
        maxSendResponseWait = conf.maxSendResponseWait;
        Arrays.setAll(workersPool, workerIndex -> new Worker(completionQueue,workerIndex, context, issues, conf));
        LOG.debug("AnodotTargetID: " + targetIdentifier + " | Created ParallelSender with workers:" + parallelSendersNumber + " Properties field path: " + propertiesFieldPath + " partitionKeyPath: " + partitionFieldKeyPath);
    }

    public void init(String resolvedUrl, String contentType, MultivaluedMap<String, Object> resolvedHeaders) {
        if(completionQueue.size() != 0) {//This check is more efficient than clearing an empty queue
            LOG.warn("AnodotTargetID: " + targetIdentifier + " | Resetting non-empty completion queue on initialization, current size: " + completionQueue.size());
            completionQueue.clear();
        }

        if(pendingTasks != 0) {
            LOG.warn("AnodotTargetID: " + targetIdentifier + " | Resetting pending tasks on initialization, current value: " + pendingTasks);
            pendingTasks = 0;
        }

        for(Worker worker : workersPool) {
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | Initializing worker with resolvedURL: " + resolvedUrl + " and contentType: " + contentType);
            worker.init(resolvedUrl, contentType, resolvedHeaders);
        }
    }

    public void destroy() {
        for(Worker worker : workersPool) {
            worker.destroy();
        }
    }

    public void send(Record record) {
        int targetPartition = getPartition(record);
        LOG.trace("AnodotTargetID: " + targetIdentifier + " | Partition resolved to: " + targetPartition);
        if(workersPool[targetPartition].add(record)) {
            ++pendingTasks;
        }
    }

    public void flush() {
        LOG.trace("AnodotTargetID: " + targetIdentifier + " | Flushing workers");

        for (Worker worker : workersPool) {
            if(worker.flush()) {
                ++pendingTasks;
            }
        }

        LOG.trace("AnodotTargetID: " + targetIdentifier + " | Flushing workers, final pending tasks: " + pendingTasks);
    }

    /**
     * A potentially blocking call that retrieves the next completed future of sent event. If no completed futures are available waits up to {@link #maxSendResponseWait} for the future to become available.
     * @return a completed future if one is available or null otherwise. Returning null may also mean that all futures were consumed (the happy path flow)
     * @throws InterruptedException if interrupted while waiting for future result to become available
     */
    public @Nullable Future<BatchResponse> take() throws InterruptedException {
        LOG.trace("AnodotTargetID: " + targetIdentifier + " | Trying to take completed future, pending tasks: " + pendingTasks);

        if(pendingTasks == 0) {
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | No more pending tasks");
            return null;
        }

        Future<BatchResponse> responseFuture = completionQueue.poll(maxSendResponseWait, TimeUnit.MILLISECONDS);
        if(responseFuture != null) {
            --pendingTasks;
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | Acquired next responseFuture, new pendingTasks: " + pendingTasks);
        } else {
            LOG.warn("AnodotTargetID: " + targetIdentifier + " | Timed out while waiting for responseFuture, current pending tasks: " + pendingTasks);
        }

        return responseFuture;
    }

    private int getPartition(Record record) {
        String partitionKey = Optional.ofNullable(record.get().getValueAsMap().get(propertiesFieldPath)).
                map(Field::getValueAsMap).
                map(map -> map.get(partitionFieldKeyPath)).
                map(Field::getValueAsString).
                orElse(null);

        LOG.trace("AnodotTargetID: " + targetIdentifier + " | Resolving partition for partitionKey: " + partitionKey);

        if(partitionKey == null) {
            return DEFAULT_PARTITION;
        }

        return Math.abs(partitionKey.hashCode() % parallelSendersNumber);
    }

    public class Worker {
        private final CompletionService<BatchResponse> completionService ;
        private final DataGeneratorFactory generatorFactory;
        private final HttpClientCommon httpClientCommon;
        private       List<Record> currentBatch = new ArrayList<>();
        //We could have relied on completionQueue size instead of this counter but working with non-atomic variable is faster
        private       Invocation.Builder builder;
        private final AnodotTargetConfig conf;
        private       String contentType;
        private final int   maxBatchSize;
        private final int   workerIndex;
        public Worker(BlockingQueue<Future<BatchResponse>> completionQueue, int workerIndex, Target.Context context, List<Stage.ConfigIssue> issues, AnodotTargetConfig conf) {
            completionService = new ExecutorCompletionService<>(Executors.newSingleThreadExecutor(), completionQueue);
            this.workerIndex = workerIndex;
            this.conf = conf;
            this.maxBatchSize = conf.maxBatchSize;
            this.httpClientCommon = new HttpClientCommon(this.conf.client);
            this.httpClientCommon.init(issues, context);
            this.generatorFactory = this.conf.dataGeneratorFormatConfig.getDataGeneratorFactory();
        }

        public void init(String resolvedUrl, String contentType, MultivaluedMap<String, Object> resolvedHeaders) {
            this.builder = createBuilder(resolvedUrl, resolvedHeaders);
            this.contentType = contentType;
        }

        /**
         * Adds the provided record to worker's batch. If the current batch exceeds the maximum batch size after the addition, the batch is actually sent to destination, otherwise no sending is performed
         * @param record a record to send
         * @return true if the batch was actually sent to destination, false otherwise
         */
        public boolean add(Record record) {
            currentBatch.add(record);
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Adding new record to batch");
            if(currentBatch.size() < maxBatchSize) {
                LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Batch too small, not sending");
                return false;
            }

            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Sending batch to target");
            return submitCurrentBatch();
        }

        /**
         * Forces the worker to send the current batch regardless of its size
         * @return true if the batch was sent (if it was non-empty), false otherwise
         */
        public boolean flush() {
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Performing flush on currentBatch of size: " + currentBatch.size());
            return submitCurrentBatch();
        }

        private boolean submitCurrentBatch() {
            if(currentBatch.isEmpty()) {
                LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Received batch is empty, returning");
                return false;
            }
            List<Record> batchToSend = currentBatch;
            currentBatch = new ArrayList<>();

            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Going to send batch of size: " + batchToSend.size());
            completionService.submit(() -> sendRecords(batchToSend));
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Submitted batch for sending");
            return true;
        }

        public BatchResponse sendRecords(@NotNull List<Record> currentBatch) {
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Sending records from batch of size: " + currentBatch.size());
            StreamingOutput streamingOutput = createStreamingOutput(currentBatch.iterator());
            Response response = builder.method(conf.httpMethod.getLabel(), Entity.entity(streamingOutput, contentType));
            String responseEntity = null;
            if (conf.client.useOAuth2 && (response.getStatus() == 403 || response.getStatus() == 401)) {
                refreshAuthToken();
                response = builder.method(conf.httpMethod.getLabel(), Entity.entity(streamingOutput, contentType));
                if (response.hasEntity()) {
                    responseEntity = response.readEntity(String.class);
                }
            }
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Received response for request: " + response.getStatus());
            return new BatchResponse(response, currentBatch, responseEntity);
        }

        private StreamingOutput createStreamingOutput(Iterator<Record> records) {
            return outputStream -> writeToStream(records, outputStream);
        }

        public void writeToStream(Iterator<Record> records, OutputStream outputStream) throws IOException {
            try (DataGenerator dataGenerator = generatorFactory.getGenerator(outputStream)) {
                while (records.hasNext()) {
                    Record record = records.next();
                    dataGenerator.write(record);
                    LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Wrote record to dataGenerator");
                }
                dataGenerator.flush();
            } catch (DataGeneratorException e) {
                throw new IOException(e);
            }
        }

        private Invocation.Builder createBuilder(String resolvedUrl, MultivaluedMap<String, Object> resolvedHeaders) throws StageException {
            WebTarget target = httpClientCommon.getClient().target(resolvedUrl);
            // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request
            if (httpClientCommon.requestContainsSensitiveInfo(conf.headers, null) &&
                    !target.getUri().getScheme().toLowerCase().startsWith("https")) {
                throw new StageException(Errors.HTTP_07);
            }
            return target.request()
                    .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, httpClientCommon.getAuthToken()).headers(resolvedHeaders);
        }

        private void refreshAuthToken() {
            HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, httpClientCommon.getClient());
        }

        public void destroy() {
            httpClientCommon.destroy();
            LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Worker destroyed");
        }
    }

    class BatchResponse {
        private final Response response;
        private final List<Record> currentBatch;
        private final String responseEntity;
        public BatchResponse(Response response, List<Record> currentBatch, String responseEntity) {
            this.response = response;
            this.currentBatch = currentBatch;
            this.responseEntity = responseEntity;
        }

        public Response getResponse() {
            return response;
        }

        public List<Record> getCurrentBatch() {
            return currentBatch;
        }

        public String getResponseEntity() {
            return responseEntity;
        }
    }
}
