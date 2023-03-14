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

package org.apache.hudi.tdbank;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.tdbank.busapi.BusClientConfig;
import com.tencent.tdbank.busapi.DefaultMessageSender;
import com.tencent.tdbank.busapi.MessageSender;
import com.tencent.tdbank.busapi.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TDBankClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TDBankClient.class);
  private static final Long TDBANK_SENDER_TIMEOUT_MS =
      Long.parseLong(System.getProperty("tdbank.sender.timeout-ms", "20000"));
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String HUDI_EVENT_TID = "hudi_metric";

  private final String bid;
  private MessageSender sender;
  private final String tdmAddr;
  private final int tdmPort;
  private volatile boolean hasInit = false;

  private static final int RETRY_TIMES = 3;

  public TDBankClient(String tdmAddr, int tdmPort, String bid) {
    this.bid = bid;
    this.tdmAddr = tdmAddr;
    this.tdmPort = tdmPort;
  }

  /**
   * send message to tdbank and return send result
   */
  public SendResult sendMessage(Object message) {
    try {
      init();
      LOG.info("Send message to tdbank, bid: {}, tid: {}", bid, HUDI_EVENT_TID);
      int retryTimes = 0;
      while (retryTimes < RETRY_TIMES) {
        try {
          if (sender != null) {
            return sender.sendMessage(MAPPER.writeValueAsBytes(message),
                bid, HUDI_EVENT_TID, 0, UUID.randomUUID().toString(), TDBANK_SENDER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          }
        } catch (Exception e) {
          retryTimes++;
          LOG.error("Error when send data to tdbank retry " + retryTimes, e);
        }
      }
    } catch (Exception e) {
      LOG.error("Error when send data to tdbank", e);
    }
    return SendResult.UNKOWN_ERROR;
  }

  private void init() {
    if (!hasInit) {
      synchronized (this) {
        if (!hasInit) {
          try {
            LOG.info("Init tdbank-client with tdmAddress: {}, tdmPort: {}, bid: {}", tdmAddr, tdmPort, bid);
            String localhost = InetAddress.getLocalHost().getHostAddress();
            BusClientConfig clientConfig =
                new BusClientConfig(localhost, true, tdmAddr, tdmPort, bid, "all");
            LOG.info("Before sender generated.");
            sender = DefaultMessageSender.generateSenderByClusterId(clientConfig);
            LOG.info("Successfully init sender.");
          } catch (Exception e) {
            sender = new MockMessageSender();
            LOG.warn("Failed to initialize tdbank client, using mock client instead. "
                + "Warn: using mock client will ignore all the incoming events", e);
          }
          hasInit = true;
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (sender != null) {
      sender.close();
    }
  }
}
