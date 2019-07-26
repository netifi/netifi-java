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

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.annotation.BrokerClient;

public interface DestinationAwareClientFactory<T> extends BrokerClientFactory<T> {
  default T destination() {
    return lookup(BrokerClient.Type.DESTINATION);
  }

  default T destination(String destination) {
    return lookup(BrokerClient.Type.DESTINATION, Tags.of("com.netifi.destination", destination));
  }

  default T destination(String destination, String group) {
    return lookup(BrokerClient.Type.DESTINATION, group, Tags.of("com.netifi.destination", destination));
  }
}
