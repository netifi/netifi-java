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

package com.netifi.spring.messaging;

import static com.netifi.spring.core.annotation.BrokerClientStaticFactory.resolveTags;
import static com.netifi.spring.messaging.RSocketRequesterStaticFactory.createRSocketRequester;
import static com.netifi.spring.messaging.RSocketRequesterStaticFactory.resolveBrokerClientRSocket;
import static org.springframework.core.annotation.AnnotatedElementUtils.getMergedAnnotation;

import com.netifi.broker.BrokerService;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.RSocket;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.reactive.HandlerMethodArgumentResolver;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

public class BrokerClientRequesterMethodArgumentResolver implements HandlerMethodArgumentResolver {

  private final BrokerService brokerClient;
  private final DefaultListableBeanFactory listableBeanFactory;
  private final RSocketStrategies rSocketStrategies;
  private final MeterRegistry registry;
  private final Tracer tracer;

  public BrokerClientRequesterMethodArgumentResolver(
      BrokerService client,
      DefaultListableBeanFactory factory,
      RSocketStrategies strategies,
      MeterRegistry registry,
      Tracer tracer) {
    this.brokerClient = client;
    this.listableBeanFactory = factory;
    this.rSocketStrategies = strategies;
    this.registry = registry;
    this.tracer = tracer;
  }

  @Override
  public boolean supportsParameter(MethodParameter parameter) {
    Class<?> type = parameter.getParameterType();
    return (RSocketRequester.class.equals(type) || RSocket.class.isAssignableFrom(type));
  }

  @Override
  public Mono<Object> resolveArgument(MethodParameter parameter, Message<?> message) {
    Class<?> type = parameter.getParameterType();
    com.netifi.spring.core.annotation.BrokerClient brokerClientAnnotation =
        getMergedAnnotation(
            parameter.getParameter(), com.netifi.spring.core.annotation.BrokerClient.class);

    Assert.notNull(
        brokerClientAnnotation,
        "Incorrect Method Parameter, make sure your parameter is annotated with the @BrokerClient annotation");

    if (RSocketRequester.class.equals(type)) {
      return Mono.just(
          createRSocketRequester(
              brokerClient,
              brokerClientAnnotation,
              resolveTags(listableBeanFactory, brokerClientAnnotation),
              rSocketStrategies,
              registry,
              tracer));
    } else if (RSocket.class.isAssignableFrom(type)) {
      return Mono.just(
          resolveBrokerClientRSocket(
              brokerClient,
              brokerClientAnnotation.type(),
              brokerClientAnnotation.group(),
              brokerClientAnnotation.destination(),
              resolveTags(listableBeanFactory, brokerClientAnnotation)));
    } else {
      return Mono.error(new IllegalArgumentException("Unexpected parameter type: " + parameter));
    }
  }
}
