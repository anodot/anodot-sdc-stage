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
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
@StageDef(
    version = 1,
    label = "Anodot Destination",
    description = "",
    icon = "anodot_logo.png",
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class AnodotDTarget extends AnodotTarget {

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          label = "Anodot URL",
          description = "Anodot metrics API URL with token",
          evaluation = ConfigDef.Evaluation.EXPLICIT,
          displayPosition = 60,
          group = "ANODOT"
  )
  public String resourceUrl = "";

  /** {@inheritDoc} */
  @Override
  public String getResourceUrl() {
    return resourceUrl;
  }
}
