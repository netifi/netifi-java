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
