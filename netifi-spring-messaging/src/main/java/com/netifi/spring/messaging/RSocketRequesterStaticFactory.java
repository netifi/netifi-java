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
package com.netifi.spring.messaging;

import com.netifi.broker.BrokerService;
import com.netifi.broker.rsocket.BrokerSocket;
import com.netifi.common.tags.Tags;
import com.netifi.spring.core.annotation.BrokerClientStaticFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;

class RSocketRequesterStaticFactory {

  static RSocketRequester createRSocketRequester(
      BrokerService brokerClient,
      com.netifi.spring.core.annotation.BrokerClient brokerClientAnnotation,
      Tags tags,
      RSocketStrategies rSocketStrategies,
      MeterRegistry meterRegistry,
      Tracer tracer) {
    if (meterRegistry != null && tracer != null) {
      return new MetricsAwareRSocketRequester(
          RSocketRequester.wrap(
              resolveBrokerClientRSocket(
                  brokerClient,
                  brokerClientAnnotation.type(),
                  brokerClientAnnotation.group(),
                  brokerClientAnnotation.destination(),
                  tags),
              MimeTypeUtils.ALL,
              MimeTypeUtils.ALL,
              rSocketStrategies),
          rSocketStrategies,
          meterRegistry,
          tracer);
    }
    return RSocketRequester.wrap(
        resolveBrokerClientRSocket(
            brokerClient,
            brokerClientAnnotation.type(),
            brokerClientAnnotation.group(),
            brokerClientAnnotation.destination(),
            tags),
        MimeTypeUtils.ALL,
        MimeTypeUtils.ALL,
        rSocketStrategies);
  }

  static RSocketRequester createRSocketRequester(
      BrokerService brokerClient,
      com.netifi.spring.core.annotation.BrokerClient.Type routingType,
      String group,
      String destination,
      Tags tags,
      RSocketStrategies rSocketStrategies,
      MeterRegistry meterRegistry,
      Tracer tracer) {

    if (meterRegistry != null && tracer != null) {
      return new MetricsAwareRSocketRequester(
          RSocketRequester.wrap(
              resolveBrokerClientRSocket(brokerClient, routingType, group, destination, tags),
              MimeTypeUtils.ALL,
              MimeTypeUtils.ALL,
              rSocketStrategies),
          rSocketStrategies,
          meterRegistry,
          tracer);
    } else {
      return RSocketRequester.wrap(
          resolveBrokerClientRSocket(brokerClient, routingType, group, destination, tags),
          MimeTypeUtils.ALL,
          MimeTypeUtils.ALL,
          rSocketStrategies);
    }
  }

  static BrokerSocket resolveBrokerClientRSocket(
      BrokerService brokerClient,
      com.netifi.spring.core.annotation.BrokerClient.Type routingType,
      String group,
      String destination,
      Tags tags) {
    return BrokerClientStaticFactory.createBrokerRSocket(
        brokerClient, routingType, group, destination, tags);
  }
}
