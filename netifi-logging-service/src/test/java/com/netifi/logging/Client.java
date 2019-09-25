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
