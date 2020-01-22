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
package com.netifi.broker.rsocket;

import com.netifi.broker.frames.*;
import com.netifi.broker.frames.AuthorizationWrapperFlyweight;
import com.netifi.broker.frames.BroadcastFlyweight;
import com.netifi.broker.frames.FrameHeaderFlyweight;
import com.netifi.broker.frames.FrameType;
import com.netifi.broker.frames.GroupFlyweight;
import com.netifi.broker.frames.ShardFlyweight;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.ByteBufPayload;

// Need to unwrap RSocketRpc Messages
public class UnwrappingRSocket extends AbstractUnwrappingRSocket {

  public UnwrappingRSocket(RSocket source) {
    super(source);
  }

  @Override
  protected Payload unwrap(Payload payload) {
    try {
      ByteBuf data = payload.sliceData();
      ByteBuf metadata = payload.sliceMetadata();
      FrameType frameType = FrameHeaderFlyweight.frameType(metadata);
      ByteBuf unwrappedMetadata = unwrapMetadata(frameType, metadata);
      return ByteBufPayload.create(data.retain(), unwrappedMetadata.retain());
    } finally {
      payload.release();
    }
  }

  private ByteBuf unwrapMetadata(FrameType frameType, ByteBuf metadata) {
    switch (frameType) {
      case AUTHORIZATION_WRAPPER:
        ByteBuf innerFrame = AuthorizationWrapperFlyweight.innerFrame(metadata);
        return unwrapMetadata(FrameHeaderFlyweight.frameType(innerFrame), innerFrame);
      case GROUP:
        return GroupFlyweight.metadata(metadata);
      case BROADCAST:
        return BroadcastFlyweight.metadata(metadata);
      case SHARD:
        return ShardFlyweight.metadata(metadata);
      default:
        throw new IllegalStateException("unknown frame type " + frameType);
    }
  }
}
