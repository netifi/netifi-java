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
package com.netifi.broker.rsocket.transport;

import com.netifi.broker.info.Broker;
import java.net.InetSocketAddress;
import java.util.function.Function;

public class BrokerAddressSelectors {
  public static Function<Broker, InetSocketAddress> TCP_ADDRESS =
      broker -> InetSocketAddress.createUnresolved(broker.getTcpAddress(), broker.getTcpPort());
  public static Function<Broker, InetSocketAddress> WEBSOCKET_ADDRESS =
      broker ->
          InetSocketAddress.createUnresolved(
              broker.getWebSocketAddress(), broker.getWebSocketPort());
}
