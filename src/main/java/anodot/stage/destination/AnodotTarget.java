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
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.http.Errors;
import com.streamsets.pipeline.lib.http.JerseyClientConfigBean;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.glassfish.jersey.client.oauth1.OAuth1ClientSupport;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * This target is an example and does not actually write to any destination.
 */
public class AnodotTarget extends BaseTarget {

    private static final Logger LOG = LoggerFactory.getLogger(AnodotTarget.class);
    private final AnodotTargetConfig conf;
    private final HttpClientCommon httpClientCommon;
    private DataGeneratorFactory generatorFactory;
    private ErrorRecordHandler errorRecordHandler;

    private static final int BATCH_SIZE = 1000;

    protected AnodotTarget(AnodotTargetConfig conf) {
        this.conf = conf;
        this.httpClientCommon = new HttpClientCommon(conf.client);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
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
            generatorFactory = conf.dataGeneratorFormatConfig.getDataGeneratorFactory();
        }
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
        super.destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(Batch batch) throws StageException {
        try {
            Iterator<Record> records = batch.getRecords();
            Record lastRecord = getLastRecord(batch);
            if (lastRecord != null && !conf.agentOffsetUrl.equals("")) {
                String pipelineId = lastRecord.get().getValueAsMap().get("tags").getValueAsMap().get("pipeline_id").getValueAsList().get(0).getValueAsString();
                String offset = lastRecord.get().getValueAsMap().get("timestamp").getValueAsString();
                sendOffsetToAgent(offset, pipelineId);
            }

            while (records.hasNext()) {
                // Use first record for resolving url, headers, ...
                Record firstRecord = batch.getRecords().next();
                MultivaluedMap<String, Object> resolvedHeaders = httpClientCommon.resolveHeaders(conf.headers, firstRecord);
                Invocation.Builder builder = getBuilder(firstRecord).headers(resolvedHeaders);
                String contentType = HttpStageUtil.getContentType(resolvedHeaders, conf.dataFormat);

                ArrayList<Record> currentBatch = new ArrayList<>();
                StreamingOutput streamingOutput = getStreamingOutput(records, currentBatch);
                Response response = builder.method(conf.httpMethod.getLabel(), Entity.entity(streamingOutput, contentType));

                String responseBody = "";
                if (conf.client.useOAuth2 && (response.getStatus() == 403 || response.getStatus() == 401)) {
                    HttpStageUtil.getNewOAuth2Token(conf.client.oauth2, httpClientCommon.getClient());
                } else if (response.getStatus() < 200 || response.getStatus() >= 300) {
                    errorRecordHandler.onError(
                            currentBatch,
                            new OnRecordErrorException(
                                    com.streamsets.pipeline.lib.http.Errors.HTTP_40,
                                    response.getStatus(),
                                    response.getStatusInfo().getReasonPhrase() + " " + responseBody
                            )
                    );
                } else if (response.hasEntity()) {
                    responseBody = response.readEntity(String.class);
                    processErrors(responseBody, currentBatch);
                }
                response.close();
            }
        } catch (Exception ex) {
            LOG.error(com.streamsets.pipeline.lib.http.Errors.HTTP_41.getMessage(), ex.toString(), ex);
            errorRecordHandler.onError(Lists.newArrayList(batch.getRecords()), new StageException(Errors.HTTP_41, ex, ex));
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

    private void sendOffsetToAgent(String offset, String pipelineId) throws Exception {
        HttpClientCommon httpClientCommon = new HttpClientCommon(new JerseyClientConfigBean());
        List<ConfigIssue> issues = super.init();
        httpClientCommon.init(issues, getContext());
        WebTarget target = httpClientCommon.getClient().target(conf.agentOffsetUrl + pipelineId);
        Invocation.Builder builder = target.request();
        MultivaluedMap<String, Object> requestHeaders = new MultivaluedHashMap<>();
        String contentType = HttpStageUtil.getContentType(requestHeaders, DataFormat.JSON);
        Response response = builder.method(String.valueOf(HttpMethod.POST), Entity.entity(String.format("{\"offset\": \"%s\"}", offset).getBytes(StandardCharsets.UTF_8), contentType));

        if (response.getStatus() < 200 || response.getStatus() >= 300) {
            String responseEntity = response.readEntity(String.class);
            response.close();
            throw new Exception("Failed to save agent offset, response: " + responseEntity);
        }
        response.close();
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

    private StreamingOutput getStreamingOutput(Iterator<Record> records, List<Record> currentBatch) {
        return outputStream -> {
            try (DataGenerator dataGenerator = generatorFactory.getGenerator(outputStream)) {
                int batchRecordsNum = 0;
                while (records.hasNext() && batchRecordsNum < BATCH_SIZE) {
                    Record record = records.next();
                    dataGenerator.write(record);
                    currentBatch.add(record);
                    batchRecordsNum++;
                }
                dataGenerator.flush();
            } catch (DataGeneratorException e) {
                throw new IOException(e);
            }
        };
    }


    private Invocation.Builder getBuilder(Record record) throws StageException {
        String resolvedUrl = httpClientCommon.getResolvedUrl(conf.resourceUrl, record);
        WebTarget target = httpClientCommon.getClient().target(resolvedUrl);
        // If the request (headers or body) contain a known sensitive EL and we're not using https then fail the request
        if (httpClientCommon.requestContainsSensitiveInfo(conf.headers, null) &&
                !target.getUri().getScheme().toLowerCase().startsWith("https")) {
            throw new StageException(Errors.HTTP_07);
        }
        return target.request()
                .property(OAuth1ClientSupport.OAUTH_PROPERTY_ACCESS_TOKEN, httpClientCommon.getAuthToken());
    }

}
