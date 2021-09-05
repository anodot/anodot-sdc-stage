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

    private final String targetIdentifier;//Helps us to track the instance through multithreaded environment
    private final String propertiesFieldPath;
    private final String partitionFieldKeyPath;
    private final int parallelSendersNumber;
    private static final int DEFAULT_PARTITION = 0;
    private final Worker[] workersPool;

    public ParallelSender(AnodotTargetConfig conf,
                          Target.Context context,
                          List<Stage.ConfigIssue> issues, String targetIdentifier) {
        parallelSendersNumber = conf.parallelSendersNumber;
        this.targetIdentifier = targetIdentifier;
        workersPool = new Worker[parallelSendersNumber];
        propertiesFieldPath = conf.propertiesPath;
        partitionFieldKeyPath = conf.partitioningKeyPath;
        Arrays.setAll(workersPool, workerIndex -> new Worker(workerIndex, context, issues, conf));
        //LOG.debug("AnodotTargetID: " + targetIdentifier + " | Created ParallelSender with workers:" + parallelSendersNumber + " Properties field path: " + propertiesFieldPath + " partitionKeyPath: " + partitionFieldKeyPath);
    }

    public void init(String resolvedUrl, String contentType, MultivaluedMap<String, Object> resolvedHeaders) {
        for(Worker worker : workersPool) {
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | Initializing worker with resolvedURL: " + resolvedUrl + " and contentType: " + contentType);
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
        //LOG.trace("AnodotTargetID: " + targetIdentifier + " | Partition resolved to: " + targetPartition);
        workersPool[targetPartition].offer(record);
    }

    public void flush() {
        //LOG.trace("AnodotTargetID: " + targetIdentifier + " | Flushing workers");

        for (Worker worker : workersPool) {
            worker.flush();
        }
    }

    public @Nullable Future<BatchResponse> poll() throws InterruptedException {
        //First we try to darin in non-blocking mode
        for(Worker worker : workersPool) {
            Future<BatchResponse> completedFuture = worker.poll();
            if(completedFuture != null) {
                return completedFuture;
            }
        }

        //If no completed futures are available ATM we start to block until a future is ready or none pending tasks are left
        for(Worker worker : workersPool) {
            Future<BatchResponse> responseFuture = worker.take();

            if(responseFuture != null) {
                return responseFuture;
            }
        }

        return null;
    }

    private int getPartition(Record record) {
        String partitionKey = Optional.ofNullable(record.get().getValueAsMap().get(propertiesFieldPath)).
                map(Field::getValueAsMap).
                map(map -> map.get(partitionFieldKeyPath)).
                map(Field::getValueAsString).
                orElse(null);

        //LOG.trace("AnodotTargetID: " + targetIdentifier + " | Resolving partition for partitionKey: " + partitionKey);

        if(partitionKey == null) {
            return DEFAULT_PARTITION;
        }

        return Math.abs(partitionKey.hashCode() % parallelSendersNumber);
    }

    public class Worker {
        private final ExecutorService exec = Executors.newSingleThreadExecutor();
        private final BlockingQueue<Future<BatchResponse>> completionQueue = new LinkedBlockingQueue<>();
        private final CompletionService<BatchResponse> completionService = new ExecutorCompletionService<>(exec, completionQueue);
        private final DataGeneratorFactory generatorFactory;
        private final HttpClientCommon httpClientCommon;
        private       List<Record> currentBatch = new ArrayList<>();
        //We could have relied on completionQueue size instead of this counter but working with non-atomic variable is faster
        private       long pendingTasks = 0;
        private       Invocation.Builder builder;
        private final AnodotTargetConfig conf;
        private       String contentType;
        private final int   maxBatchSize;
        private final int   workerIndex;
        public Worker(int workerIndex, Target.Context context, List<Stage.ConfigIssue> issues, AnodotTargetConfig conf) {
            this.workerIndex = workerIndex;
            this.conf = conf;
            this.maxBatchSize = conf.maxBatchSize;
            this.httpClientCommon = new HttpClientCommon(this.conf.client);
            this.httpClientCommon.init(issues, context);
            this.generatorFactory = this.conf.dataGeneratorFormatConfig.getDataGeneratorFactory();
        }

        public void init(String resolvedUrl, String contentType, MultivaluedMap<String, Object> resolvedHeaders) {
            if(completionQueue.size() != 0) {//This check is more efficient than clearing an empty queue
                LOG.warn("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Completion queue non-empty on initialization: " + completionQueue.size());

                completionQueue.clear();
            }

            this.builder = createBuilder(resolvedUrl, resolvedHeaders);
            this.contentType = contentType;
        }

        public void offer(Record record) {
            currentBatch.add(record);
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Adding new record to batch");
            if(currentBatch.size() < maxBatchSize) {
                //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Batch too small, not sending");
                return;
            }

            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Sending batch to target");
            submitCurrentBatch();
        }

        public void flush() {
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Performing flush on currentBatch of size: " + currentBatch.size());
            submitCurrentBatch();
        }

        public @Nullable Future<BatchResponse> poll() {
            Future<BatchResponse> responseFuture = completionService.poll();
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Polling completed future, result is : " + responseFuture + ", pending tasks: " + pendingTasks);
            if(responseFuture != null) {
                --pendingTasks;
            }
            return responseFuture;
        }

        public @Nullable Future<BatchResponse> take() throws InterruptedException {
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Trying to take completed future, pending tasks: " + pendingTasks);

            if(pendingTasks == 0) {
                return null;
            }

            Future<BatchResponse> responseFuture = completionService.poll(1, TimeUnit.MINUTES);
            if(responseFuture == null) {
                LOG.error("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Waiting for send to complete timed out, pending tasks: " + pendingTasks + ", completion queue size: " + completionQueue.size());
            }

            --pendingTasks;
            return responseFuture;
        }

        private void submitCurrentBatch() {
            if(currentBatch.isEmpty()) {
                //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Received batch is empty, returning");
                return;
            }
            List<Record> batchToSend = currentBatch;
            currentBatch = new ArrayList<>();

            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Going to send batch of size: " + batchToSend.size());
            completionService.submit(() -> sendRecords(batchToSend));
            ++pendingTasks;
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Submitted batch for sending");
        }

        public BatchResponse sendRecords(@NotNull List<Record> currentBatch) {
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Sending records from batch of size: " + currentBatch.size());
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
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Received response for request: " + response.getStatus());
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
                    //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Wrote record to dataGenerator");
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
            //LOG.trace("AnodotTargetID: " + targetIdentifier + " | worker: " + workerIndex + " | Worker destroyed");
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
