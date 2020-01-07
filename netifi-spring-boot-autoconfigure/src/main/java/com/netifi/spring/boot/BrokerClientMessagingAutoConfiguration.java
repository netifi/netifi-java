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

import com.netifi.broker.BrokerService;
import com.netifi.spring.messaging.BrokerClientRequesterMethodArgumentResolver;
import com.netifi.spring.messaging.MessagingRSocketRequesterClientFactory;
import com.netifi.spring.messaging.MessagingRouter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.ipc.MutableRouter;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.util.Optional;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.rsocket.RSocketProperties;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.rsocket.context.RSocketServerBootstrap;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.messaging.rsocket.annotation.support.RSocketRequesterMethodArgumentResolver;
import org.springframework.util.MimeTypeUtils;

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
  public MutableRouter messagingCustomizer(
      RSocketProperties rSocketProperties,
      BrokerClientProperties brokerClientProperties,
      BrokerClientMessagingProperties properties,
      DefaultListableBeanFactory factory,
      RSocketStrategies rSocketStrategies,
      RSocketMessageHandler handler) {
    return new MessagingRouter(
        MimeTypeUtils.ALL,
        MimeTypeUtils.ALL,
        rSocketStrategies.metadataExtractor(),
        handler,
        rSocketStrategies.routeMatcher(),
        rSocketStrategies);
  }

  @Bean
  @ConditionalOnMissingBean
  public RSocketServerBootstrap messageHandlerAcceptor(
      BrokerClientMessagingProperties properties,
      DefaultListableBeanFactory factory,
      RSocketStrategies rSocketStrategies,
      RSocketMessageHandler handler,
      BrokerService brokerClient,
      Optional<MeterRegistry> registry,
      Optional<Tracer> tracer) {
    RSocketServerBootstrap bootstrap = new NetifiBootstrap(brokerClient);

    handler
        .getArgumentResolverConfigurer()
        .getCustomResolvers()
        .removeIf(r -> r instanceof RSocketRequesterMethodArgumentResolver);

    handler
        .getArgumentResolverConfigurer()
        .addCustomResolver(
            new BrokerClientRequesterMethodArgumentResolver(
                brokerClient,
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
