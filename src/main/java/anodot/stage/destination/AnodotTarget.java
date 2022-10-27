/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package anodot.stage.destination;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.JerseyClientConfigBean;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * This target is an example and does not actually write to any destination.
 */
public class AnodotTarget extends BaseTarget {

    private static final Logger LOG = LoggerFactory.getLogger(AnodotTarget.class);
    private final AnodotTargetConfig conf;
    private final HttpClientCommon httpClientCommon;
    private ErrorRecordHandler errorRecordHandler;
    private ParallelSender parallelSender;
    private final String targetIdentifier;//Helps us to track the instance through multithreaded environment

    protected AnodotTarget(AnodotTargetConfig conf) {
        targetIdentifier = UUID.randomUUID().toString();
        LOG.info("AnodotTargetID: " + targetIdentifier + " | Creating AnodotTarget");
        this.conf = conf;
        this.httpClientCommon = new HttpClientCommon(conf.client);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        LOG.info("AnodotTargetID: " + targetIdentifier + " | Initializing AnodotTarget");

        List<ConfigIssue> issues = super.init();
        errorRecordHandler = new DefaultErrorRecordHandler(getContext());
        this.httpClientCommon.init(issues, getContext());
        if (issues.size() == 0) {
            conf.dataGeneratorFormatConfig.init(
                    getContext(),
                    conf.dataFormat,
                    Groups.HTTP.name(),
                    HttpClientCommon.DATA_FORMAT_CONFIG_PREFIX,
                    issues
            );
        }

        parallelSender = new ParallelSender(conf, getContext(), issues, targetIdentifier);
        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        // Clean up any open resources.
        this.httpClientCommon.destroy();
        this.parallelSender.destroy();
        super.destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(Batch batch) throws StageException {
        try {
            LOG.debug("AnodotTargetID: " + targetIdentifier + " | Starting new batch");
            Iterator<Record> records = batch.getRecords();
            if (!records.hasNext()) {
                LOG.debug("AnodotTargetID: " + targetIdentifier + " | Received empty batch");
                return;
            }

            // Use first record for resolving url, headers, ...
            Record firstRecord = batch.getRecords().next();
            MultivaluedMap<String, Object> resolvedHeaders = httpClientCommon.resolveHeaders(conf.headers, firstRecord);
            String resolvedUrl = httpClientCommon.getResolvedUrl(conf.resourceUrl, firstRecord);
            String contentType = HttpStageUtil.getContentType(resolvedHeaders, conf.dataFormat);

            LOG.debug("AnodotTargetID: " + targetIdentifier + " | Initialising parallel sender with resolvedURL: " + resolvedUrl + " and contentType: " + contentType);

            parallelSender.init(resolvedUrl, contentType, resolvedHeaders);
            long recordCounter = 0;
            while (records.hasNext()) {
                Record record = records.next();
                if (LOG.isTraceEnabled()) {
                    LOG.trace("AnodotTargetID: " + targetIdentifier + " | Sending next record: " + record);
                }

                parallelSender.send(record);
                ++recordCounter;
            }

            LOG.debug("AnodotTargetID: " + targetIdentifier + " | Finished sending messages, sent: " + recordCounter);
            parallelSender.flush();

            int processedTasksCount = 0;

            for (Future<ParallelSender.BatchResponse> responseFuture = parallelSender.take(); responseFuture != null; responseFuture = parallelSender.take()) {
                ++processedTasksCount;
                processResponse(responseFuture);
            }

            LOG.debug("AnodotTargetID: " + targetIdentifier + " | Finished processing futures, total: " + processedTasksCount);

            Record lastRecord = getLastRecord(batch);
            if (lastRecord != null) {
                if (!conf.agentOffsetUrl.equals("")) {
                    String offset = lastRecord.get().getValueAsMap().get("timestamp").getValueAsString();//properties/what
                    sendToUrl(String.format("{\"offset\": \"%s\"}", offset), conf.agentOffsetUrl);
                }
                if (!conf.agentWatermarkUrl.equals("")) {
                    String watermark = lastRecord.get().getValueAsMap().get("watermark").getValueAsString();
                    sendToUrl(String.format("{\"watermark\": \"%s\"}", watermark), conf.agentWatermarkUrl);
                }
            }
        } catch (TimeoutException ex) {
            LOG.error(com.streamsets.pipeline.lib.http.Errors.HTTP_41.getMessage(), ex, ex);
            throw new RuntimeException(ex);
        } catch (Exception ex) {
            LOG.error(com.streamsets.pipeline.lib.http.Errors.HTTP_41.getMessage(), ex, ex);
            errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), new StageException(Errors.HTTP_41, ex, ex));
        }
    }

    private void processResponse(Future<ParallelSender.BatchResponse> batchResponseFuture) {
        ParallelSender.BatchResponse batchResponse;
        try {
            batchResponse = batchResponseFuture.get();
        } catch (InterruptedException e) {
            LOG.error("AnodotTargetID: " + targetIdentifier + " | interrupted while waiting for response", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LOG.error("AnodotTargetID: " + targetIdentifier + " | Execution exception encountered while sending to anodot, aborting: ", e);
            throw new RuntimeException(e);
        }
        Response response = batchResponse.getResponse();

        try {
            List<Record> currentBatch = batchResponse.getCurrentBatch();
            if (response.getStatus() < 200 || response.getStatus() >= 300) {
                errorRecordHandler.onError(
                        currentBatch,
                        new OnRecordErrorException(
                                com.streamsets.pipeline.lib.http.Errors.HTTP_40,
                                response.getStatus(),
                                response.getStatusInfo().getReasonPhrase()
                        )
                );
                return;
            }
            String responseAsString = response.readEntity(String.class);
            if (!responseAsString.equals("")) {
                processErrors(responseAsString, currentBatch);
            }
        } finally {
            response.close();
        }
    }

    private Record getLastRecord(Batch batch) {
        Iterator<Record> iter = batch.getRecords();
        Record record = null;
        while (iter.hasNext()) {
            record = iter.next();
        }
        return record;
    }

    private void sendToUrl(String payload, String url) throws Exception {
        HttpClientCommon httpClientCommon = new HttpClientCommon(new JerseyClientConfigBean());
        httpClientCommon.init(super.init(), getContext());
        Invocation.Builder builder = httpClientCommon.getClient().target(url).request();
        Response response = builder.method(
                String.valueOf(HttpMethod.POST),
                Entity.entity(
                        payload.getBytes(StandardCharsets.UTF_8),
                        HttpStageUtil.getContentType(new MultivaluedHashMap<>(), DataFormat.JSON)
                )
        );

        try {
            if (response.getStatus() < 200 || response.getStatus() >= 300) {
                String responseEntity = response.readEntity(String.class);
                throw new Exception(
                        String.format("Failed to send request, url: %s, response: %s", url, responseEntity)
                );
            }
        } finally {
            response.close();
        }
    }

    private void processErrors(String responseBody, List<Record> currentBatch) throws StageException {
        JSONObject jsonResponse = new JSONObject(responseBody);
        JSONArray errors = (JSONArray) jsonResponse.get("errors");
        for (int i = 0; i < errors.length(); i++) {
            JSONObject error = errors.getJSONObject(i);
            if (error.has("index")) {
                errorRecordHandler.onError(
                        new OnRecordErrorException(
                                currentBatch.get(error.getInt("index")),
                                anodot.stage.lib.Errors.ANODOT_01,
                                error.getInt("error"),
                                error.getString("description")
                        )
                );
            } else {
                errorRecordHandler.onError(
                        currentBatch,
                        new OnRecordErrorException(
                                anodot.stage.lib.Errors.ANODOT_01,
                                error.getInt("error"),
                                error.getString("description")
                        )
                );
            }
        }
    }
}
