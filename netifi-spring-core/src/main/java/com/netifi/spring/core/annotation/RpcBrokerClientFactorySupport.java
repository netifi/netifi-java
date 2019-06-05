package com.netifi.spring.core.annotation;

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BrokerClientFactorySupport;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.RSocket;

public class RpcBrokerClientFactorySupport implements BrokerClientFactorySupport {

  private final com.netifi.broker.BrokerClient brokerClient;
  private final Tracer tracer;
  private final MeterRegistry meterRegistry;

  public RpcBrokerClientFactorySupport(
      com.netifi.broker.BrokerClient client, MeterRegistry registry, Tracer tracer) {
    brokerClient = client;
    this.tracer = tracer;
    meterRegistry = registry;
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
          brokerClient, type, group, null, tag, tracer, meterRegistry, clientClass);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Error instantiating Netifi Broker Client for '%s'", clientClass.getSimpleName()),
          e);
    }
  }
}
