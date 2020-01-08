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

package com.netifi.spring.boot.messaging;

import com.netifi.broker.BrokerService;
import com.netifi.spring.boot.BrokerClientAutoConfiguration;
import com.netifi.spring.messaging.BrokerClientRequesterMethodArgumentResolver;
import com.netifi.spring.messaging.MessagingRSocketRequesterClientFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.util.Optional;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.messaging.rsocket.annotation.support.RSocketRequesterMethodArgumentResolver;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Spring RSocket support in Spring
 * Messaging.
 *
 * @author Oleh Dokuka
 * @since 1.7.0
 */
@Configuration
@ConditionalOnClass({RSocketRequester.class, RSocketFactory.class, TcpServerTransport.class})
@AutoConfigureAfter({RSocketStrategiesAutoConfiguration.class, BrokerClientAutoConfiguration.class})
@EnableConfigurationProperties(BrokerClientMessagingProperties.class)
public class BrokerMessagingAutoConfiguration {

  private static final RSocket STUB_RSOCKET = new AbstractRSocket() {};

  @Bean
  public NetifiBootstrap netifiBootstrap(
      DefaultListableBeanFactory factory,
      RSocketStrategies rSocketStrategies,
      RSocketMessageHandler handler,
      BrokerService brokerService,
      Optional<MeterRegistry> registry,
      Optional<Tracer> tracer) {
    NetifiBootstrap bootstrap = new NetifiBootstrap(brokerService);

    handler
        .getArgumentResolverConfigurer()
        .getCustomResolvers()
        .removeIf(r -> r instanceof RSocketRequesterMethodArgumentResolver);

    handler
        .getArgumentResolverConfigurer()
        .addCustomResolver(
            new BrokerClientRequesterMethodArgumentResolver(
                brokerService,
                factory,
                rSocketStrategies,
                registry.orElse(null),
                tracer.orElse(null)));

    return bootstrap;
  }

  @Bean
  public MessagingRSocketRequesterClientFactory messagingRSocketRequesterClientFactory(
      BrokerClientMessagingProperties properties,
      BrokerService brokerClient,
      RSocketStrategies rSocketStrategies,
      Optional<MeterRegistry> registry,
      Optional<Tracer> tracer) {
    return new MessagingRSocketRequesterClientFactory(
        brokerClient, registry.orElse(null), tracer.orElse(null), rSocketStrategies);
  }
}
