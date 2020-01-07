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

import static com.netifi.spring.messaging.RSocketRequesterStaticFactory.createRSocketRequester;

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BrokerClientFactorySupport;
import com.netifi.spring.core.annotation.BrokerClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;

public class MessagingRSocketRequesterClientFactory implements BrokerClientFactorySupport {

  private final com.netifi.broker.BrokerService brokerClient;
  private final Tracer tracer;
  private final MeterRegistry meterRegistry;
  private final RSocketStrategies rSocketStrategies;

  public MessagingRSocketRequesterClientFactory(
      com.netifi.broker.BrokerService brokerClient,
      MeterRegistry meterRegistry,
      Tracer tracer,
      RSocketStrategies strategies) {
    this.brokerClient = brokerClient;
    this.tracer = tracer;
    this.meterRegistry = meterRegistry;
    this.rSocketStrategies = strategies;
  }

  @Override
  public boolean support(Class<?> clazz) {
    return clazz.equals(RSocketRequester.class) || RSocketRequester.class.isAssignableFrom(clazz);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T lookup(
      Class<T> tClass, BrokerClient.Type type, String methodGroup, Tags methodTags) {
    return (T)
        createRSocketRequester(
            brokerClient,
            type,
            methodGroup,
            null,
            methodTags,
            rSocketStrategies,
            meterRegistry,
            tracer);
  }
}
