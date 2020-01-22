/*
 *    Copyright 2020 The Netifi Authors
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
package com.netifi.spring.core.annotation;

import com.netifi.broker.BrokerService;
import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BrokerClientFactorySupport;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.RSocket;

public class RpcBrokerClientFactorySupport implements BrokerClientFactorySupport {

  private final com.netifi.broker.BrokerService brokerService;
  private final Tracer tracer;
  private final MeterRegistry meterRegistry;

  public RpcBrokerClientFactorySupport(
      BrokerService brokerService, MeterRegistry registry, Tracer tracer) {
    this.brokerService = brokerService;
    this.tracer = tracer;
    this.meterRegistry = registry;
  }

  @Override
  public boolean support(Class<?> clazz) {
    boolean thereIsRSocketConstructor;
    try {
      clazz.getConstructor(RSocket.class);
      thereIsRSocketConstructor = true;
    } catch (NoSuchMethodException e) {
      thereIsRSocketConstructor = false;
    }
    return !clazz.isInterface() && thereIsRSocketConstructor;
  }

  @Override
  public <T> T lookup(Class<T> clientClass, BrokerClient.Type type, String group, Tags tag) {
    try {
      return BrokerClientStaticFactory.createBrokerClient(
          brokerService, type, group, null, tag, tracer, meterRegistry, clientClass);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Error instantiating Netifi Broker Client for '%s'", clientClass.getSimpleName()),
          e);
    }
  }
}
