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
package com.netifi.spring.core;

import com.netifi.broker.RoutingBrokerService;
import io.rsocket.rpc.RSocketRpcService;
import java.util.Map;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

public class BrokerClientApplicationEventListener {
  private final RoutingBrokerService brokerClient;

  public BrokerClientApplicationEventListener(RoutingBrokerService brokerClient) {
    this.brokerClient = brokerClient;
  }

  @EventListener
  public void onApplicationEvent(ContextRefreshedEvent event) {
    ApplicationContext context = event.getApplicationContext();
    Map<String, RSocketRpcService> rSocketServiceMap =
        context.getBeansOfType(RSocketRpcService.class);

    rSocketServiceMap.values().forEach(s -> s.selfRegister(brokerClient.router()));
  }
}
