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

import io.rsocket.rpc.annotations.internal.Generated;
import io.rsocket.rpc.annotations.internal.ResourceType;

@Generated(type = ResourceType.CLIENT, idlClass = TestIdl.class)
public class DefaultClientTestIdl implements TestIdl {

  public DefaultClientTestIdl(io.rsocket.RSocket rSocket) {}

  public DefaultClientTestIdl(
      io.rsocket.RSocket rSocket, io.micrometer.core.instrument.MeterRegistry registry) {}

  public DefaultClientTestIdl(io.rsocket.RSocket rSocket, io.opentracing.Tracer tracer) {}

  public DefaultClientTestIdl(
      io.rsocket.RSocket rSocket,
      io.micrometer.core.instrument.MeterRegistry registry,
      io.opentracing.Tracer tracer) {}
}
