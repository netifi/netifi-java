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

import com.netifi.broker.info.BrokerInfoService;
import com.netifi.broker.info.BrokerInfoServiceClient;
import com.netifi.spring.DefaultExternalIdlClient;
import com.netifi.spring.core.annotation.Broadcast;
import com.netifi.spring.core.annotation.BrokerClient;
import com.netifi.spring.core.annotation.Destination;
import com.netifi.spring.core.annotation.Group;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandler;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandlerClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = TestableConfiguration.class)
public class PlainAutowireIntegrationTest {

  @Autowired ConfigurableListableBeanFactory beanFactory;

  @Broadcast("test")
  TestIdl broadcastTestIdlClient;

  @Group("test")
  TestIdl groupTestIdlClient;

  @Destination(group = "test", destination = "test")
  TestIdl destinationTestIdaClient;

  @Group("test")
  MetricsSnapshotHandler metricsSnapshotHandlerClient;

  @Group("test")
  BrokerInfoService brokerInfoServiceClient;

  @Destination(group = "test", destination = "test")
  DefaultExternalIdlClient defaultExternalIdlClient;

  @Autowired
  @BrokerClient(group = "test", destination = "test", clientClass = DefaultExternalIdlClient.class)
  DestinationAwareClientFactory<DefaultExternalIdlClient> destinationAwareClientFactory;

  @Autowired
  @BrokerClient(group = "test", clientClass = DefaultExternalIdlClient.class)
  GroupAwareClientFactory<DefaultExternalIdlClient> groupAwareClientFactory;

  @Autowired
  @BrokerClient(group = "test", clientClass = DefaultExternalIdlClient.class)
  BroadcastAwareClientFactory<DefaultExternalIdlClient> broadcastAwareClientFactory;

  @Autowired TestIdl serviceImpl;

  @Test
  public void shouldFindGeneratedBean() {
    Assertions.assertEquals(DefaultClientTestIdl.class, broadcastTestIdlClient.getClass());
    Assertions.assertEquals(DefaultClientTestIdl.class, groupTestIdlClient.getClass());
    Assertions.assertEquals(DefaultClientTestIdl.class, destinationTestIdaClient.getClass());
    Assertions.assertEquals(
        MetricsSnapshotHandlerClient.class, metricsSnapshotHandlerClient.getClass());
    Assertions.assertEquals(BrokerInfoServiceClient.class, brokerInfoServiceClient.getClass());
    Assertions.assertEquals(TestIdlImpl.class, serviceImpl.getClass());
    Assertions.assertNotNull(destinationAwareClientFactory);
    Assertions.assertNotNull(groupAwareClientFactory);
    Assertions.assertNotNull(broadcastAwareClientFactory);
    Assertions.assertNotNull(defaultExternalIdlClient);
  }
}
