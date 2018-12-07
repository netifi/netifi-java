package io.netifi.proteus.rsocket;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.rpc.RSocketRpcService;
import io.rsocket.rpc.frames.Metadata;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.RSocketProxy;
import reactor.core.publisher.Flux;

public class NamedRSocketServiceWrapper extends AbstractUnwrappingRSocket
    implements RSocketRpcService {
  private final String name;

  private NamedRSocketServiceWrapper(String name, RSocket source) {
    super(source);
    this.name = name;
  }

  /**
   * Wraps an RSocket with {@link RSocketProxy} and {@link RSocketRpcService}
   *
   * @param name what you want your RSocket to be found as
   * @param source the raw socket to handle to wrap
   * @return a new NamedRSocketServiceWrapper instance
   */
  public static NamedRSocketServiceWrapper wrap(String name, RSocket source) {
    return new NamedRSocketServiceWrapper(name, source);
  }

  @Override
  protected Payload unwrap(Payload payload) {
    try {
      ByteBuf data = payload.sliceData();
      ByteBuf metadata = payload.sliceMetadata();
      ByteBuf unwrappedMetadata = Metadata.getMetadata(metadata);

      return ByteBufPayload.create(data.retain(), unwrappedMetadata.retain());
    } finally {
      payload.release();
    }
  }

  @Override
  public String getService() {
    return name;
  }

  @Override
  public final Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher) {
    return reactor.core.publisher.Flux.error(
        new UnsupportedOperationException("Request-Channel not implemented."));
  }
}
