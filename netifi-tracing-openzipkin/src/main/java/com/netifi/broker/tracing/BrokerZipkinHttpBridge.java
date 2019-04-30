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
package com.netifi.broker.tracing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.netifi.broker.BrokerClient;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import java.time.Duration;
import java.util.Optional;
import java.util.StringJoiner;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import zipkin2.proto3.Span;

public class BrokerZipkinHttpBridge implements BrokerTracingService {
  private static final Logger logger = LoggerFactory.getLogger(BrokerZipkinHttpBridge.class);

  private static final String DEFAULT_ZIPKIN_SPANS_URL = "/api/v2/spans";
  private static final String DEFAULT_ZIPKIN_TRACES_URL = "/api/v2/traces";

  private final String host;
  private final int port;
  private final String zipkinSpansUrl;
  private HttpClient httpClient;

  public BrokerZipkinHttpBridge(String host, int port, String zipkinSpansUrl) {
    this.host = host;
    this.port = port;
    this.zipkinSpansUrl = zipkinSpansUrl;
  }

  public BrokerZipkinHttpBridge(String host, int port) {
    this(host, port, DEFAULT_ZIPKIN_SPANS_URL);
  }

  public static void main(String... args) {
    logger.info("Starting Stand-alone Broker Zipkin HTTP Bridge");

    String group = System.getProperty("netifi.tracingGroup", "com.netifi.tracing");
    String brokerHost = System.getProperty("netifi.host", "localhost");
    int brokerPort = Integer.getInteger("netifi.port", 8001);
    String zipkinHost = System.getProperty("netifi.zipkinHost", "localhost");
    int zipkinPort = Integer.getInteger("netifi.zipkinPort", 9411);
    String zipkinSpansUrl = System.getProperty("netifi.zipkinSpansUrl", DEFAULT_ZIPKIN_SPANS_URL);
    long accessKey = Long.getLong("netifi.accessKey", 3855261330795754807L);
    String accessToken =
        System.getProperty("netifi.authentication.accessToken", "kTBDVtfRBO4tHOnZzSyY5ym2kfY");

    logger.info("group - {}", group);
    logger.info("broker host - {}", brokerHost);
    logger.info("broker port - {}", brokerPort);
    logger.info("zipkin host - {}", zipkinHost);
    logger.info("zipkin port - {}", zipkinPort);
    logger.info("zipkin spans url - {}", zipkinSpansUrl);
    logger.info("access key - {}", accessKey);

    BrokerClient brokerClient =
        BrokerClient.tcp()
            .accessKey(accessKey)
            .accessToken(accessToken)
            .group(group)
            .host(brokerHost)
            .port(brokerPort)
            .destination("standaloneZipkinBridge")
            .build();

    brokerClient.addService(
        new BrokerTracingServiceServer(
            new BrokerZipkinHttpBridge(zipkinHost, zipkinPort, zipkinSpansUrl),
            Optional.empty(),
            Optional.empty()));

    brokerClient.onClose().block();
  }

  private synchronized HttpClient getClient() {
    if (httpClient == null) {
      this.httpClient =
          HttpClient.create(ConnectionProvider.fixed("brokerZipkinBridge"))
              .compress(true)
              .port(port)
              .tcpConfiguration(
                  tcpClient ->
                      tcpClient
                          .host(host)
                          .option(ChannelOption.SO_KEEPALIVE, true)
                          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30_000));
    }
    return httpClient;
  }

  private synchronized void resetHttpClient() {
    this.httpClient = null;
  }

  @Override
  public Mono<Ack> streamSpans(Publisher<Span> messages, ByteBuf metadata) {
    return Flux.from(messages)
        .map(
            span -> {
              try {
                String json = JsonFormat.printer().print(span);
                if (logger.isTraceEnabled()) {
                  logger.trace("receiving tracing data {}", json);
                }
                return json;
              } catch (InvalidProtocolBufferException e) {
                throw Exceptions.propagate(e);
              }
            })
        .windowTimeout(128, Duration.ofMillis(1000))
        .map(
            strings ->
                strings
                    .reduce(new StringJoiner(","), StringJoiner::add)
                    .map(stringJoiner -> "[" + stringJoiner.toString() + "]"))
        .onBackpressureBuffer(1 << 16)
        .flatMap(stringMono -> stringMono)
        .concatMap(
            spans ->
                getClient()
                    .headers(hh -> hh.add("Content-Type", "application/json"))
                    .post()
                    .uri(zipkinSpansUrl)
                    .send(ByteBufFlux.fromString(Mono.just(spans)))
                    .response()
                    .timeout(Duration.ofSeconds(30))
                    .doOnError(throwable -> resetHttpClient()),
            8)
        .doOnError(
            throwable ->
                logger.error(
                    "error sending data to tracing data to url " + zipkinSpansUrl, throwable))
        .then(Mono.never());
  }
}
