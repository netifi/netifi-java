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
package com.netifi.spring.boot.test;

import static org.mockito.ArgumentMatchers.any;

import com.netifi.broker.BrokerClient;
import com.netifi.broker.BrokerFactory;
import com.netifi.broker.BrokerService;
import com.netifi.spring.boot.support.BrokerClientConfigurer;
import com.netifi.spring.boot.support.BrokerServiceConfigurer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ClientConfigurationIntegrationTest {

  @Autowired
  @Qualifier("mock2")
  BrokerClientConfigurer configurer;

  @Autowired
  @Qualifier("mock3")
  BrokerServiceConfigurer brokerServiceConfigurer;

  @Autowired BrokerClient brokerClient;
  @Autowired BrokerService brokerService;

  @Test
  public void testThatConfigurerWorks() {
    Assertions.assertNotNull(brokerClient);
    //    ArgumentCaptor<BrokerClient.CustomizableBuilder> captor =
    //        ArgumentCaptor.forClass(BrokerClient.CustomizableBuilder.class);
    ArgumentCaptor captor = ArgumentCaptor.forClass(Object.class);

    // fixme
    //    Mockito.verify(configurer).configure(captor.capture());
    Mockito.verify(brokerServiceConfigurer)
        .configure((BrokerFactory.ClientBuilder) captor.capture());

    Assertions.assertNotNull(captor.getValue());
  }

  @org.springframework.boot.test.context.TestConfiguration
  //  @ComponentScan
  static class TestConfiguration {

    @Bean
    @Qualifier("mock2")
    public BrokerClientConfigurer testBrokerClientConfigurer() {
      BrokerClientConfigurer configurer = Mockito.mock(BrokerClientConfigurer.class);

      Mockito.when(configurer.configure(any(BrokerClient.CustomizableBuilder.class)))
          .then(a -> a.getArgument(0));

      return configurer;
    }

    @Bean
    @Qualifier("mock3")
    public BrokerServiceConfigurer testBrokerServiceConfigurer() {

      return Mockito.mock(BrokerServiceConfigurer.class);
    }
  }
}
