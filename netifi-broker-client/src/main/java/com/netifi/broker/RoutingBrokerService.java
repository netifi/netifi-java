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
package com.netifi.broker;

import io.rsocket.Payload;
import io.rsocket.ipc.MutableRouter;
import io.rsocket.ipc.util.IPCChannelFunction;
import io.rsocket.ipc.util.IPCFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RoutingBrokerService<SELF extends RoutingBrokerService<SELF>>
    extends BrokerService {
  MutableRouter router();

  default SELF withFireAndForgetHandler(String route, IPCFunction<Mono<Void>> handler) {
    router().withFireAndForgetRoute(route, handler);
    return (SELF) this;
  }

  default SELF withRequestResponseHandler(String route, IPCFunction<Mono<Payload>> handler) {
    router().withRequestResponseRoute(route, handler);
    return (SELF) this;
  }

  default SELF withRequestStreamHandler(String route, IPCFunction<Flux<Payload>> handler) {
    router().withRequestStreamRoute(route, handler);
    return (SELF) this;
  }

  default SELF withRequestChannelHandler(String route, IPCChannelFunction handler) {
    router().withRequestChannelRoute(route, handler);
    return (SELF) this;
  }
}
