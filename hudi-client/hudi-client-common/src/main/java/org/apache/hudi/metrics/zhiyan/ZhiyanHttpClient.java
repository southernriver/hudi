/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metrics.zhiyan;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ZhiyanHttpClient {

  private static final Logger LOG = LogManager.getLogger(ZhiyanHttpClient.class);
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;
  private final String serviceUrl;
  private final String requestPath;

  private static final String JSON_CONTENT_TYPE = "application/json";
  private static final String CONTENT_TYPE = "Content-Type";

  public ZhiyanHttpClient(String url, String path, int timeoutSeconds) {
    httpClient = HttpClientBuilder.create()
      .setDefaultRequestConfig(RequestConfig.custom()
        .setConnectTimeout(timeoutSeconds * 1000)
        .setConnectionRequestTimeout(timeoutSeconds * 1000)
        .setSocketTimeout(timeoutSeconds * 1000).build())
      .build();

    serviceUrl = url;
    requestPath = path;

    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    mapper.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  }

  public <T> String post(T input) throws Exception {
    HttpPost postReq = new HttpPost(serviceUrl + requestPath);
    postReq.setHeader(CONTENT_TYPE, JSON_CONTENT_TYPE);

    try {
      return requestWithEntity(postReq, input);
    } catch (Exception e) {
      LOG.warn(String.format("Failed to post to %s, cause by", serviceUrl + requestPath), e);
      throw e;
    } finally {
      postReq.releaseConnection();
    }
  }

  private <T> String requestWithEntity(HttpRequestBase request, T input) throws Exception {
    if (input != null && request instanceof HttpEntityEnclosingRequestBase) {
      HttpEntity entity = getEntity(input);
      ((HttpEntityEnclosingRequestBase) request).setEntity(entity);
    }

    HttpContext httpContext = new BasicHttpContext();
    try (CloseableHttpResponse response = httpClient.execute(request, httpContext)) {
      int status = response.getStatusLine().getStatusCode();
      if (status != HttpStatus.SC_OK && status != HttpStatus.SC_CREATED) {
        throw new HttpException("Response code is " + status);
      }
      HttpEntity resultEntity = response.getEntity();
      return EntityUtils.toString(resultEntity, Consts.UTF_8);
    } catch (Exception ex) {
      LOG.error("Error when request http.", ex);
      throw ex;
    }
  }

  private <T> HttpEntity getEntity(T input) throws JsonProcessingException {
    HttpEntity entity;
    if (input instanceof String) {
      entity = new StringEntity((String) input, ContentType.APPLICATION_JSON);
    } else if (input instanceof HttpEntity) {
      return (HttpEntity) input;
    } else {
      try {
        String json = mapper.writeValueAsString(input);
        entity = new StringEntity(json, ContentType.APPLICATION_JSON);
      } catch (JsonProcessingException e) {
        LOG.error(String.format("Error when process %s due to ", input), e);
        throw e;
      }
    }
    return entity;
  }

  public void close() {
    try {
      httpClient.close();
    } catch (IOException e) {
      LOG.error("failed to close http client", e);
    }
  }
}
