package io.netifi.proteus.discovery;

import io.netifi.proteus.common.net.HostAndPort;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class StaticListDiscoveryStrategy implements DiscoveryStrategy {
  private static final Logger logger = LoggerFactory.getLogger(StaticListDiscoveryStrategy.class);

  private final StaticListDiscoveryConfig staticListDiscoveryConfig;
  private Mono<? extends Collection<HostAndPort>> nodes;

  public StaticListDiscoveryStrategy(StaticListDiscoveryConfig staticListDiscoveryConfig) {
    this.staticListDiscoveryConfig = staticListDiscoveryConfig;
    this.nodes =
        Mono.defer(
                () -> {
                  if (this.staticListDiscoveryConfig.getAddresses().isEmpty()) {
                    return Mono.empty();
                  } else {
                    logger.debug(
                        "seeding cluster with {}", this.staticListDiscoveryConfig.getAddresses());
                    return Flux.fromIterable(this.staticListDiscoveryConfig.getAddresses())
                        .map(
                            hostPortString ->
                                HostAndPort.fromString(hostPortString)
                                    .withDefaultPort(this.staticListDiscoveryConfig.getPort()))
                        .collectList();
                  }
                })
            .cache();
  }

  @Override
  public Mono<? extends Collection<HostAndPort>> discoverNodes() {
    return nodes;
  }
}
