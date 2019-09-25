/*
 *    Copyright 2019 The Netifi Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.netifi.logging;

import com.google.protobuf.StringValue;
import com.netifi.broker.BrokerClient;
import com.netifi.broker.rsocket.BrokerSocket;

public class Client {

  private static long accessKey = Long.getLong("ACCESS_KEY", 9007199254740991L);
  private static String accessToken =
      System.getProperty("ACCESS_TOKEN", "kTBDVtfRBO4tHOnZzSyY5ym2kfY=");

  public static void main(String... args) {
    BrokerClient client =
        BrokerClient.tcp()
            .host("localhost")
            .port(8001)
            .accessKey(accessKey)
            .accessToken(accessToken)
            .group("Client")
            .disableSsl()
            .build();

    BrokerSocket brokerSocket = client.group("TestLogger");
    LoggingServiceClient client1 = new LoggingServiceClient(brokerSocket);
    client1
        .streamLogByAppenderName(StringValue.newBuilder().setValue("NetifiGatewayAppender").build())
        .doOnNext(
            logEvents -> {
              System.out.println(logEvents.toString());
            })
        .blockLast();
  }
}
