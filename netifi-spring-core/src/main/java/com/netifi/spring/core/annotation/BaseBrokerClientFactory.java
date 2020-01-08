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
package com.netifi.spring.core.annotation;

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BrokerClientFactory;
import com.netifi.spring.core.BrokerClientFactorySupport;
import java.util.HashMap;
import java.util.Map;

public class BaseBrokerClientFactory<T> implements BrokerClientFactory<T> {

  private final BrokerClientFactorySupport brokerClientFactorySupport;
  private final BrokerClient.Type routeType;
  private final String destination;
  private final Class<?> clientClass;
  private final String group;
  private final Tags tags;
  private final Map<String, Object> instantiatedClients;

  public BaseBrokerClientFactory(
      BrokerClientFactorySupport brokerClientFactorySupport,
      BrokerClient.Type routeType,
      String destination,
      Class<T> clientClass,
      String group,
      Tags tags) {
    this.brokerClientFactorySupport = brokerClientFactorySupport;
    this.routeType = routeType;
    this.destination = destination;
    this.clientClass = clientClass;
    this.group = group;
    this.tags = tags;
    this.instantiatedClients = new HashMap<>();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T lookup(BrokerClient.Type type, String methodGroup, Tags methodTags) {
    String key = key(type, methodGroup, methodTags);
    T client;
    if (instantiatedClients.containsKey(key)) {
      client = (T) instantiatedClients.get(key);
    } else {
      try {
        client = (T) brokerClientFactorySupport.lookup(clientClass, type, methodGroup, methodTags);
        instantiatedClients.put(key, client);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Error instantiating Netifi Broker Client for '%s'", clientClass.getSimpleName()),
            e);
      }
    }
    return client;
  }

  @Override
  public T lookup(BrokerClient.Type type, String methodGroup, String... methodTags) {
    return lookup(type, methodGroup, Tags.of(methodTags));
  }

  @Override
  public T lookup(BrokerClient.Type type) {
    return lookup(type, group, tags);
  }

  @Override
  public T lookup(BrokerClient.Type type, Tags methodTags) {
    return lookup(type, group, methodTags);
  }

  @Override
  public T lookup(String group, Tags tags) {
    return lookup(routeType, group, tags);
  }

  @Override
  public T lookup(String group, String... tags) {
    return lookup(routeType, group, tags);
  }

  @Override
  public T lookup(Tags tags) {
    return lookup(routeType, tags);
  }

  @Override
  public T lookup() {
    return lookup(routeType);
  }

  // TODO: Do something smarter
  private String key(BrokerClient.Type type, String group, Tags tags) {
    return type.name() + group + destination + tags.hashCode();
  }
}
