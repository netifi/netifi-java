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

import com.netifi.broker.BrokerClient;
import com.netifi.broker.RoutingBrokerService;
import com.netifi.spring.core.config.EnableBrokerClient;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBrokerClient
public class TestableConfiguration {

  @Bean
  public BrokerClient brokerClient() {
    return Mockito.mock(BrokerClient.class);
  }

  @Bean
  public RoutingBrokerService routingBrokerService() {
    return Mockito.mock(RoutingBrokerService.class);
  }

  @Bean
  public TestIdlImpl testIdlImpl() {
    return new TestIdlImpl();
  }
}
