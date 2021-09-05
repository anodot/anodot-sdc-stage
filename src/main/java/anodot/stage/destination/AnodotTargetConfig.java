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

import com.streamsets.pipeline.api.ConfigDef;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.VaultEL;
import com.streamsets.pipeline.lib.http.JerseyClientConfigBean;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.destination.lib.ToOriginResponseConfig;

import java.util.HashMap;
import java.util.Map;


public class AnodotTargetConfig {

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Anodot URL",
          description = "Anodot metrics API URL with token",
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          displayPosition = 60,
          group = "HTTP"
  )
  public String resourceUrl = "";

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.MODEL,
          defaultValue = "JSON",
          label = "Data Format",
          description = "Data Format of the response. Response will be parsed before being placed in the record.",
          displayPosition = 1,
          group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDefBean(groups = {"DATA_FORMAT"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;


  @ConfigDefBean
  public JerseyClientConfigBean client = new JerseyClientConfigBean();


  @ConfigDef(
          required = false,
          type = ConfigDef.Type.MAP,
          label = "Headers",
          description = "Headers to include in the request",
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          displayPosition = 70,
          elDefs = {RecordEL.class, VaultEL.class},
          group = "HTTP"
  )
  public Map<String, String> headers = new HashMap<>();

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.MODEL,
          label = "HTTP Method",
          defaultValue = "POST",
          description = "HTTP method to send",
          elDefs = RecordEL.class,
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          displayPosition = 80,
          group = "HTTP"
  )
  @ValueChooserModel(HttpMethodChooserValues.class)
  public HttpMethod httpMethod = HttpMethod.POST;

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          label = "HTTP Method Expression",
          description = "Expression used to determine the HTTP method to use",
          displayPosition = 90,
          dependsOn = "httpMethod",
          elDefs = RecordEL.class,
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          triggeredByValue = { "EXPRESSION" },
          group = "HTTP"
  )
  public String methodExpression = "";


  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "true",
          label = "One Request per Batch",
          description = "Generates a single HTTP request with all records in the batch",
          displayPosition = 141,
          group = "HTTP"
  )
  public boolean singleRequestPerBatch;

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.NUMBER,
          label = "Rate Limit (Requests/sec)",
          defaultValue = "0",
          description = "Maximum requests per second (0 for unlimited). Useful for rate-limited APIs.",
          displayPosition = 160,
          group = "HTTP"
  )
  public int rateLimit;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.NUMBER,
          label = "Maximum Request Time (sec)",
          defaultValue = "60",
          description = "Maximum time to wait for each request completion.",
          displayPosition = 250,
          group = "HTTP"
  )
  public long maxRequestCompletionSecs = 60L;

  @ConfigDefBean(groups = {"RESPONSE"})
  public ToOriginResponseConfig responseConf = new ToOriginResponseConfig();

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          label = "Agent offset URL",
          description = "Agent URL to send pipeline offset to",
          displayPosition = 100,
          group = "HTTP"
  )
  public String agentOffsetUrl = "";

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.NUMBER,
          label = "Senders",
          defaultValue = "10",
          description = "Number of parallel senders to send requests",
          displayPosition = 110,
          group = "HTTP"
  )
  public int parallelSendersNumber = 10;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.NUMBER,
          label = "BatchSize",
          defaultValue = "1000",
          description = "Maximum number of samples in a single batch",
          displayPosition = 120,
          group = "HTTP"
  )
  public int maxBatchSize = 1000;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "PropertiesPath",
          defaultValue = "properties",
          description = "Path to properties field in sample",
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          displayPosition = 130,
          group = "HTTP"
  )
  public String propertiesPath = "properties";

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "PartitioningKeyPath",
          defaultValue = "what",
          description = "Path to property according to which we partition sending, usually metric name or what",
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          displayPosition = 140,
          group = "HTTP"
  )
  public String partitioningKeyPath = "what";

}
