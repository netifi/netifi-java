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
import com.netifi.spring.core.BroadcastAwareClientFactory;
import com.netifi.spring.core.BrokerClientFactory;

class DefaultBroadcastAwareClientFactory<T> implements BroadcastAwareClientFactory<T> {

  private final BrokerClientFactory<T> baseFactory;

  DefaultBroadcastAwareClientFactory(BrokerClientFactory<T> factory) {
    baseFactory = factory;
  }

  @Override
  public T lookup(BrokerClient.Type type, String group, Tags tags) {
    return baseFactory.lookup(type, group, tags);
  }

  @Override
  public T lookup(BrokerClient.Type type, String group, String... tags) {
    return baseFactory.lookup(type, group, tags);
  }

  @Override
  public T lookup(BrokerClient.Type type) {
    return baseFactory.lookup(type);
  }

  @Override
  public T lookup(BrokerClient.Type type, Tags tags) {
    return baseFactory.lookup(type, tags);
  }

  @Override
  public T lookup(String group, Tags tag) {
    return baseFactory.lookup(group, tag);
  }

  @Override
  public T lookup(String group, String... tags) {
    return baseFactory.lookup(group, tags);
  }

  @Override
  public T lookup(Tags tags) {
    return baseFactory.lookup(tags);
  }

  @Override
  public T lookup() {
    return baseFactory.lookup();
  }
}
