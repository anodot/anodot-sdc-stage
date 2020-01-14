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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.http.AuthenticationFailureException;
import com.streamsets.pipeline.lib.http.oauth2.OAuth2ConfigBean;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.http.Errors.HTTP_21;
import static com.streamsets.pipeline.lib.http.Errors.HTTP_22;

public abstract class HttpStageUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HttpStageUtil.class);

  public static final String CONTENT_TYPE_HEADER = "Content-Type";
  public static final String DEFAULT_CONTENT_TYPE = "application/json";

  public static Object getFirstHeaderIgnoreCase(String name, MultivaluedMap<String, Object> headers) {
    for (final Map.Entry<String, List<Object>> headerEntry : headers.entrySet()) {
      if (name.equalsIgnoreCase(headerEntry.getKey())) {
        if (headerEntry.getValue() != null && headerEntry.getValue().size() > 0) {
          return headerEntry.getValue().get(0);
        }
        break;
      }
    }
    return null;
  }

  public static String getContentType(MultivaluedMap<String, Object> headers, DataFormat dataFormat) {
    final Object contentTypeObj = getFirstHeaderIgnoreCase(CONTENT_TYPE_HEADER, headers);
    if (contentTypeObj != null) {
      return contentTypeObj.toString();
    }

    switch (dataFormat) {
      case TEXT:
        return MediaType.TEXT_PLAIN;
      case BINARY:
        return MediaType.APPLICATION_OCTET_STREAM;
      case JSON:
      case SDC_JSON:
        return MediaType.APPLICATION_JSON;
      default:
        // Default is binary blob
        return MediaType.APPLICATION_OCTET_STREAM;
    }
  }

  public static boolean getNewOAuth2Token(OAuth2ConfigBean oauth2, Client client) throws StageException {
    LOG.info("OAuth2 Authentication token has likely expired. Fetching new token.");
    try {
      oauth2.reInit(client);
      return true;
    } catch (AuthenticationFailureException ex) {
      LOG.error("OAuth2 Authentication failed", ex);
      throw new StageException(HTTP_21);
    } catch (IOException ex) {
      LOG.error("OAuth2 Authentication Response does not contain access token", ex);
      throw new StageException(HTTP_22);
    }
  }

}
