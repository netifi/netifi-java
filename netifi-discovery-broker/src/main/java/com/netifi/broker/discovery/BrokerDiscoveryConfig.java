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
package com.netifi.broker.discovery;

import com.netifi.broker.BrokerClient;
import com.netifi.common.tags.Tags;

public class BrokerDiscoveryConfig implements DiscoveryConfig {
  private final BrokerClient client;

  private final String brokerInfoServiceGroup;

  private final Tags brokerInfoServiceTags;

  private final String clusterName;

  private final String addressTagKey;

  private final String portTagKey;

  public enum Type {
    CLUSTER("netifi.broker.clusterAddress", "netifi.broker.clusterPort"),
    TCP("netifi.broker.tcpAddress", "netifi.broker.tcpPort"),
    WS("netifi.broker.webSocketAddress", "netifi.broker.webSocketPort");

    private final String addressTagKey;
    private final String portTagKey;

    Type(String addressTagKey, String portTagKey) {
      this.addressTagKey = addressTagKey;
      this.portTagKey = portTagKey;
    }
  }

  public BrokerDiscoveryConfig(
      BrokerClient client,
      String brokerInfoServiceGroup,
      Tags brokerInfoServiceTags,
      String clusterName,
      Type type) {
    this(
        client,
        brokerInfoServiceGroup,
        brokerInfoServiceTags,
        clusterName,
        type.addressTagKey,
        type.portTagKey);
  }

  public BrokerDiscoveryConfig(
      BrokerClient client,
      String brokerInfoServiceGroup,
      Tags brokerInfoServiceTags,
      String clusterName,
      String addressTagKey,
      String portTagKey) {
    this.client = client;
    this.brokerInfoServiceGroup = brokerInfoServiceGroup;
    this.brokerInfoServiceTags = brokerInfoServiceTags;
    this.clusterName = clusterName;
    this.addressTagKey = addressTagKey;
    this.portTagKey = portTagKey;
  }

  @Override
  public Class getDiscoveryStrategyClass() {
    return BrokerDiscoveryStrategy.class;
  }

  BrokerClient getClient() {
    return client;
  }

  String getBrokerInfoServiceGroup() {
    return brokerInfoServiceGroup;
  }

  Tags getBrokerInfoServiceTags() {
    return brokerInfoServiceTags;
  }

  String getClusterName() {
    return clusterName;
  }

  String getAddressTagKey() {
    return addressTagKey;
  }

  String getPortTagKey() {
    return portTagKey;
  }
}
