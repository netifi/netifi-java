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

  private final String rSocketName;
  private final com.netifi.broker.BrokerClient brokerClient;
  private final Tracer tracer;
  private final MeterRegistry meterRegistry;
  private final RSocketStrategies rSocketStrategies;

  public MessagingRSocketRequesterClientFactory(
      String rSocketName,
      com.netifi.broker.BrokerClient brokerClient,
      MeterRegistry meterRegistry,
      Tracer tracer,
      RSocketStrategies strategies) {
    this.rSocketName = rSocketName;
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
            rSocketName,
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
