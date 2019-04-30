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
import com.netifi.spring.DefaultExternalIdlClient;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandler;

public class TestBean {

  public final TestIdl broadcastTestIdlClient;
  public final TestIdl groupTestIdlClient;
  public final TestIdl destinationTestIdaClient;
  public final MetricsSnapshotHandler metricsSnapshotHandlerClient;
  public final BrokerInfoService brokerInfoServiceClient;
  public final DefaultExternalIdlClient defaultExternalIdlClient;
  public final DestinationAwareClientFactory<DefaultExternalIdlClient>
      destinationAwareClientFactory;
  public final GroupAwareClientFactory<DefaultExternalIdlClient> groupAwareClientFactory;
  public final BroadcastAwareClientFactory<DefaultExternalIdlClient> broadcastAwareClientFactory;
  public final TestIdl serviceImpl;

  public TestBean(
      TestIdl broadcastTestIdlClient,
      TestIdl groupTestIdlClient,
      TestIdl destinationTestIdaClient,
      MetricsSnapshotHandler metricsSnapshotHandlerClient,
      BrokerInfoService brokerInfoServiceClient,
      DefaultExternalIdlClient defaultExternalIdlClient,
      DestinationAwareClientFactory<DefaultExternalIdlClient> destinationAwareClientFactory,
      GroupAwareClientFactory<DefaultExternalIdlClient> groupAwareClientFactory,
      BroadcastAwareClientFactory<DefaultExternalIdlClient> broadcastAwareClientFactory,
      TestIdl serviceImpl) {

    this.broadcastTestIdlClient = broadcastTestIdlClient;
    this.groupTestIdlClient = groupTestIdlClient;
    this.destinationTestIdaClient = destinationTestIdaClient;
    this.metricsSnapshotHandlerClient = metricsSnapshotHandlerClient;
    this.brokerInfoServiceClient = brokerInfoServiceClient;
    this.defaultExternalIdlClient = defaultExternalIdlClient;
    this.destinationAwareClientFactory = destinationAwareClientFactory;
    this.groupAwareClientFactory = groupAwareClientFactory;
    this.broadcastAwareClientFactory = broadcastAwareClientFactory;
    this.serviceImpl = serviceImpl;
  }
}
