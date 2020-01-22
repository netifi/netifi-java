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

import com.netifi.broker.frames.BroadcastFlyweight;
import com.netifi.broker.frames.GroupFlyweight;
import com.netifi.broker.frames.ShardFlyweight;
import com.netifi.broker.rsocket.BrokerSocket;
import com.netifi.broker.rsocket.DefaultBrokerSocket;
import com.netifi.common.tags.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public interface BrokerService extends Disposable, InstanceInfoAware {
  default BrokerSocket group(CharSequence group) {
    return group(group, Tags.empty());
  }

  default BrokerSocket group(CharSequence group, Tags tags) {
    return new DefaultBrokerSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              GroupFlyweight.encode(ByteBufAllocator.DEFAULT, group, metadataToWrap, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  default BrokerSocket broadcast(CharSequence group) {
    return broadcast(group, Tags.empty());
  }

  default BrokerSocket broadcast(CharSequence group, Tags tags) {
    return new DefaultBrokerSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              BroadcastFlyweight.encode(ByteBufAllocator.DEFAULT, group, metadataToWrap, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  default BrokerSocket shard(CharSequence group, ByteBuf shardKey) {
    return shard(group, shardKey, Tags.empty());
  }

  default BrokerSocket shard(CharSequence group, ByteBuf shardKey, Tags tags) {
    return new DefaultBrokerSocket(
        payload -> {
          ByteBuf data = payload.sliceData().retain();
          ByteBuf metadataToWrap = payload.sliceMetadata();
          ByteBuf metadata =
              ShardFlyweight.encode(
                  ByteBufAllocator.DEFAULT, group, metadataToWrap, shardKey, tags);
          Payload wrappedPayload = ByteBufPayload.create(data, metadata);
          payload.release();
          return wrappedPayload;
        },
        this::selectRSocket);
  }

  RSocket selectRSocket();

  Mono<Void> onClose();
}
