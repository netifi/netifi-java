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

import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import com.netifi.spring.core.annotation.BrokerClient;

public interface BroadcastAwareClientFactory<T> extends BrokerClientFactory<T> {

  default T broadcast() {
    return lookup(BrokerClient.Type.BROADCAST);
  }

  default T broadcast(String group) {
    return lookup(BrokerClient.Type.BROADCAST, group);
  }

  default T broadcast(String group, Tag... tags) {
    return lookup(BrokerClient.Type.BROADCAST, group, Tags.of(tags));
  }

  default T broadcast(String group, Tags tags) {
    return lookup(BrokerClient.Type.BROADCAST, group, tags);
  }

  default T broadcast(Tag... tags) {
    return lookup(BrokerClient.Type.BROADCAST, Tags.of(tags));
  }

  default T broadcast(Tags tags) {
    return lookup(BrokerClient.Type.BROADCAST, tags);
  }
}
