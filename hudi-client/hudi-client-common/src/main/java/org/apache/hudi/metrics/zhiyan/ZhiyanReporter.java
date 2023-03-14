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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class ZhiyanReporter extends ScheduledReporter {

  private static final Logger LOG = LoggerFactory.getLogger(ZhiyanReporter.class);
  private ZhiyanHttpClient client;
  private final String jobName;
  private final String hoodieTableName;
  private final String appMask;
  private final String seclvlEnName;

  public ZhiyanReporter(MetricRegistry registry,
                        MetricFilter filter,
                        ZhiyanHttpClient client,
                        String jobName,
                        String hoodieTableName,
                        String appMask,
                        String seclvlEnName) {
    super(registry, "hudi-zhiyan-reporter", filter, TimeUnit.SECONDS, TimeUnit.SECONDS);
    this.client = client;
    this.jobName = jobName;
    this.hoodieTableName = hoodieTableName;
    this.appMask = appMask;
    this.seclvlEnName = seclvlEnName;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    final PayloadBuilder builder = new PayloadBuilder()
        .withAppMask(appMask)
        .withJobName(jobName)
        .withSeclvlEnName(seclvlEnName)
        .withTableName(hoodieTableName);

    long timestamp = System.currentTimeMillis();

    gauges.forEach((metricName, gauge) -> {
      builder.addGauge(metricName, timestamp, gauge.getValue().toString());
    });

    String payload = builder.build();

    try {
      client.post(payload);
    } catch (Exception e) {
      LOG.error("Payload is " + payload);
      LOG.error("Error when report data to zhiyan", e);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  static class PayloadBuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ObjectNode payload;

    private final ArrayNode reportData;

    private String appMark;
    // 指标组
    private String seclvlEnName;

    private String jobName;

    private String tableName;

    public PayloadBuilder() {
      MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      MAPPER.configure(JsonParser.Feature.IGNORE_UNDEFINED, true);
      MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      this.payload = MAPPER.createObjectNode();
      this.reportData = MAPPER.createArrayNode();
    }

    PayloadBuilder withAppMask(String appMark) {
      this.appMark = appMark;
      this.payload.put("app_mark", appMark);
      return this;
    }

    PayloadBuilder withJobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    PayloadBuilder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    PayloadBuilder withSeclvlEnName(String seclvlEnName) {
      this.seclvlEnName = seclvlEnName;
      this.payload.put("sec_lvl_en_name", seclvlEnName);
      return this;
    }

    PayloadBuilder addGauge(String metric, long timestamp, String gaugeValue) {
      ObjectNode tmpData = MAPPER.createObjectNode();
      tmpData.put("metric", metric);
      tmpData.put("value", Long.parseLong(gaugeValue));
      // tags means dimension in zhiyan.
      ObjectNode tags = tmpData.objectNode();
      tags.put("jobName", jobName);
      tags.put("tableName", tableName);
      tmpData.set("tags", tags);
      this.reportData.add(tmpData);
      return this;
    }

    PayloadBuilder addHistogram() {
      return this;
    }

    PayloadBuilder addCounter() {
      return this;
    }

    PayloadBuilder addMeters() {
      return this;
    }

    String build() {
      payload.put("report_data", reportData.toString());
      return payload.toString();
    }
  }
}
