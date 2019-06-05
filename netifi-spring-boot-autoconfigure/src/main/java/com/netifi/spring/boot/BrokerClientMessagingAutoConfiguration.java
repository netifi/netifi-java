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

package com.netifi.spring.boot;

import com.netifi.broker.BrokerClient;
import com.netifi.spring.messaging.BrokerClientRequesterMethodArgumentResolver;
import com.netifi.spring.messaging.MessagingRSocketRequesterClientFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.opentracing.Tracer;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.transport.netty.server.TcpServerTransport;

import java.time.Duration;
import java.util.Optional;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.MessageHandlerAcceptor;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketRequesterMethodArgumentResolver;
import org.springframework.messaging.rsocket.RSocketStrategies;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring RSocket support in Spring
 * Messaging.
 *
 * @author Oleh Dokuka
 * @since 1.7.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({RSocketRequester.class, RSocketFactory.class, TcpServerTransport.class})
@AutoConfigureAfter({RSocketStrategiesAutoConfiguration.class, BrokerClientAutoConfiguration.class})
@EnableConfigurationProperties(BrokerClientMessagingProperties.class)
public class BrokerClientMessagingAutoConfiguration {

  private static final RSocket STUB_RSOCKET = new AbstractRSocket() {};

  @Bean
  @ConditionalOnMissingBean
  public MessageHandlerAcceptor messageHandlerAcceptor(
      BrokerClientProperties brokerClientProperties,
      BrokerClientMessagingProperties properties,
      DefaultListableBeanFactory factory,
      RSocketStrategies rSocketStrategies,
      BrokerClient brokerClient) {
    BrokerClientProperties.KeepAliveProperties keepalive =
        brokerClientProperties.getKeepalive();
    Duration tickPeriod = Duration.ofSeconds(keepalive.getTickPeriodSeconds());
    Duration ackTimeout = Duration.ofSeconds(keepalive.getAckTimeoutSeconds());
    int missedAcks = keepalive.getMissedAcks();

    ConnectionSetupPayload connectionSetupPayload =
        // FIXME: hardcoded mime for responder
        ConnectionSetupPayload.create(
            SetupFrameFlyweight.encode(
                ByteBufAllocator.DEFAULT,
                false,
                (int) tickPeriod.toMillis(),
                (int) (ackTimeout.toMillis() + tickPeriod.toMillis() * missedAcks),
                Unpooled.EMPTY_BUFFER,
                "text/plain",
                "text/plain",
                Unpooled.EMPTY_BUFFER,
                Unpooled.EMPTY_BUFFER
            )
        );
    MessageHandlerAcceptor acceptor = new MessageHandlerAcceptor();
    acceptor.setRSocketStrategies(rSocketStrategies);
    acceptor
        .getArgumentResolverConfigurer()
        .getCustomResolvers()
        .removeIf(r -> r instanceof RSocketRequesterMethodArgumentResolver);

    acceptor
        .getArgumentResolverConfigurer()
        .addCustomResolver(
            new BrokerClientRequesterMethodArgumentResolver(
                properties.getName(), brokerClient, factory, rSocketStrategies));

    brokerClient.addNamedRSocket(
        properties.getName(),
        acceptor.apply(connectionSetupPayload, STUB_RSOCKET));

    return acceptor;
  }

  @Bean
  public MessagingRSocketRequesterClientFactory messagingRSocketRequesterClientFactory(
      BrokerClientMessagingProperties properties,
      BrokerClient brokerClient,
      RSocketStrategies rSocketStrategies,
      Optional<MeterRegistry> registry,
      Optional<Tracer> tracer) {
    return new MessagingRSocketRequesterClientFactory(
        properties.getName(),
        brokerClient,
        registry.orElse(null),
        tracer.orElse(null),
        rSocketStrategies);
  }
}
