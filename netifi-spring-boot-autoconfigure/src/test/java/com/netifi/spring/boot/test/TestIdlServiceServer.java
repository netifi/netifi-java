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
package com.netifi.spring.boot.test;

import java.util.Map;

import io.rsocket.Payload;
import io.rsocket.ipc.util.IPCChannelFunction;
import io.rsocket.ipc.util.IPCFunction;
import io.rsocket.rpc.AbstractRSocketService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@javax.annotation.Generated(
    value = "by RSocket RPC proto compiler (version 0.2.2)",
    comments = "Source: isvowel.proto")
@io.rsocket.rpc.annotations.internal.Generated(
    type = io.rsocket.rpc.annotations.internal.ResourceType.SERVICE,
    idlClass = TestIdl.class)
@javax.inject.Named(value = "TestIdlServiceServer")
public class TestIdlServiceServer extends AbstractRSocketService {

  @Override
  public Class<?> getServiceClass() {
    return TestIdl.class;
  }

  @Override
  public void selfRegister(Map<String, IPCFunction<Mono<Void>>> fireAndForgetRegistry,
          Map<String, IPCFunction<Mono<Payload>>> requestResponseRegistry,
          Map<String, IPCFunction<Flux<Payload>>> requestStreamRegistry,
          Map<String, IPCChannelFunction> requestChannelRegistry) {

  }
}
