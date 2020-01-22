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
package com.netifi.broker.rsocket.ipc.decoders;

import com.netifi.broker.frames.AuthorizationWrapperFlyweight;
import com.netifi.broker.frames.BroadcastFlyweight;
import com.netifi.broker.frames.FrameHeaderFlyweight;
import com.netifi.broker.frames.FrameType;
import com.netifi.broker.frames.GroupFlyweight;
import com.netifi.broker.frames.ShardFlyweight;
import io.netty.buffer.ByteBuf;
import io.rsocket.ipc.decoders.CompositeMetadataDecoder;

public class UnwrappingDecoder extends CompositeMetadataDecoder {

  @Override
  public Metadata decode(ByteBuf metadataByteBuf) {
    FrameType frameType = FrameHeaderFlyweight.frameType(metadataByteBuf);
    return super.decode(unwrapMetadata(frameType, metadataByteBuf));
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
