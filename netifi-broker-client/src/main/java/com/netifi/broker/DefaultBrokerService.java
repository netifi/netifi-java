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
package com.netifi.broker;

import com.google.protobuf.Empty;
import com.netifi.broker.discovery.DiscoveryStrategy;
import com.netifi.broker.frames.DestinationSetupFlyweight;
import com.netifi.broker.info.Broker;
import com.netifi.broker.info.BrokerInfoServiceClient;
import com.netifi.broker.info.Event;
import com.netifi.broker.info.Id;
import com.netifi.broker.rsocket.UnwrappingRSocket;
import com.netifi.broker.rsocket.WeightedRSocket;
import com.netifi.broker.rsocket.WeightedReconnectingRSocket;
import com.netifi.broker.rsocket.transport.BrokerAddressSelectors;
import com.netifi.broker.rsocket.transport.WeightedClientTransportSupplier;
import com.netifi.common.net.HostAndPort;
import com.netifi.common.stats.FrugalQuantile;
import com.netifi.common.stats.Quantile;
import com.netifi.common.tags.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.opentracing.Tracer;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.ResponderRSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultBrokerService implements BrokerService, Disposable {
  private static final Logger logger = LoggerFactory.getLogger(DefaultBrokerService.class);
  private static final double EXP_FACTOR = 4.0;
  private static final double DEFAULT_LOWER_QUANTILE = 0.5;
  private static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  private static final int INACTIVITY_FACTOR = 500;
  private static final int EFFORT = 5;

  private final Quantile lowerQuantile = new FrugalQuantile(DEFAULT_LOWER_QUANTILE);
  private final Quantile higherQuantile = new FrugalQuantile(DEFAULT_HIGHER_QUANTILE);
  private final List<SocketAddress> seedAddresses;
  private final List<WeightedClientTransportSupplier> suppliers;
  private final List<WeightedReconnectingRSocket> members;
  private final RSocket requestHandlingRSocket;
  private final InetAddress localInetAddress;
  private final String group;
  private final boolean keepalive;
  private final long tickPeriodSeconds;
  private final long ackTimeoutSeconds;
  private final int missedAcks;
  private final long accessKey;
  private final ByteBuf accessToken;
  private final String connectionIdSeed;
  private final short additionalSetupFlags;
  private final Tags tags;
  private final List<ByteBuf> setupMetadata;

  private final Function<Broker, InetSocketAddress> addressSelector;
  private final Function<SocketAddress, ClientTransport> clientTransportFactory;
  private final int poolSize;
  private final BrokerInfoServiceClient client;
  private final MonoProcessor<Void> onClose;
  private final long selectRefreshTimeout;
  private final long selectRefreshTimeoutDuration;
  private final DiscoveryStrategy discoveryStrategy;
  private int missed = 0;
  private volatile int poolCount = 0;
  private volatile Disposable disposable;

  public DefaultBrokerService(
      List<SocketAddress> seedAddresses,
      ResponderRSocket requestHandlingRSocket,
      boolean responderRequiresUnwrapping,
      InetAddress localInetAddress,
      String group,
      Function<Broker, InetSocketAddress> addressSelector,
      Function<SocketAddress, ClientTransport> clientTransportFactory,
      int poolSize,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      long accessKey,
      ByteBuf accessToken,
      String connectionIdSeed,
      short additionalSetupFlags,
      Tags tags,
      Tracer tracer,
      DiscoveryStrategy discoveryStrategy) {

    this.discoveryStrategy = discoveryStrategy;

    if (discoveryStrategy == null) {
      if (seedAddresses.isEmpty()) {
        throw new IllegalStateException("seedAddress is empty");
      } else {
        this.seedAddresses = seedAddresses;
      }
    } else {
      this.seedAddresses = new CopyOnWriteArrayList<>();
    }

    Objects.requireNonNull(accessToken);
    if (accessToken.readableBytes() == 0) {
      throw new IllegalStateException("access token has no readable bytes");
    }

    Objects.requireNonNull(clientTransportFactory);

    this.requestHandlingRSocket =
        responderRequiresUnwrapping
            ? new UnwrappingRSocket(requestHandlingRSocket)
            : requestHandlingRSocket;
    this.localInetAddress = localInetAddress;
    this.group = group;
    this.members = Collections.synchronizedList(new ArrayList<>());
    this.suppliers = Collections.synchronizedList(new ArrayList<>());
    this.addressSelector = addressSelector;
    this.clientTransportFactory = clientTransportFactory;
    this.poolSize = poolSize;
    this.selectRefreshTimeout = System.currentTimeMillis();
    this.selectRefreshTimeoutDuration = 10_000;
    this.keepalive = keepalive;
    this.tickPeriodSeconds = tickPeriodSeconds;
    this.ackTimeoutSeconds = ackTimeoutSeconds;
    this.missedAcks = missedAcks;
    this.accessKey = accessKey;
    this.accessToken = accessToken;
    this.connectionIdSeed = connectionIdSeed;
    this.additionalSetupFlags = additionalSetupFlags;
    this.tags = tags;
    this.setupMetadata = new ArrayList<>();
    this.onClose = MonoProcessor.create();

    if (discoveryStrategy != null) {
      logger.info("discovery strategy found using " + discoveryStrategy.getClass());
      useDiscoveryStrategy();
    }

    this.client =
        new BrokerInfoServiceClient(group("com.netifi.broker.brokerServices", Tags.empty()));
    this.disposable = listenToBrokerEvents().subscribe();

    onClose
        .doFinally(
            s -> {
              if (disposable != null) {
                disposable.dispose();
              }
            })
        .subscribe();
  }

  private void useDiscoveryStrategy() {
    Mono<List<InetSocketAddress>> discoveryNodes =
        discoveryStrategy
            .discoverNodes()
            .flatMapIterable(Function.identity())
            .map(
                hostAndPort ->
                    InetSocketAddress.createUnresolved(
                        hostAndPort.getHost(), hostAndPort.getPort()))
            .collectList()
            .doOnNext(
                i -> {
                  synchronized (this) {
                    missed++;
                    seedAddresses.clear();
                    seedAddresses.addAll(i);
                  }
                })
            .doOnError(
                throwable ->
                    logger.error(
                        "error getting seed nodes using discovery strategy "
                            + discoveryStrategy.getClass(),
                        throwable));

    Disposable subscribe =
        discoveryNodes
            .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1), Duration.ofSeconds(30))
            .thenMany(
                Flux.interval(Duration.ofSeconds(10))
                    .onBackpressureDrop()
                    .concatMap(i -> discoveryNodes))
            .retryBackoff(Long.MAX_VALUE, Duration.ofSeconds(1), Duration.ofSeconds(30))
            .subscribe();

    onClose.doFinally(s -> subscribe.dispose()).subscribe();
  }

  private Supplier<Payload> createSetupPayloadSupplier(final String connectionIdSuffix) {
    final StringJoiner connectionId = new StringJoiner("-");
    if (connectionIdSeed != null) {
      connectionId.add(connectionIdSeed);
    }
    connectionId.add(connectionIdSuffix);

    final ByteBuf metadata =
        DestinationSetupFlyweight.encode(
            ByteBufAllocator.DEFAULT,
            localInetAddress,
            group,
            accessKey,
            accessToken,
            UUID.nameUUIDFromBytes(connectionId.toString().getBytes()),
            additionalSetupFlags,
            tags);
    // To release later
    this.setupMetadata.add(metadata);
    return () -> ByteBufPayload.create(Unpooled.EMPTY_BUFFER, Unpooled.copiedBuffer(metadata));
  }

  private synchronized void reconcileSuppliers(final Set<Broker> incomingBrokers) {
    if (!suppliers.isEmpty()) {
      Set<Broker> existingBrokers =
          suppliers
              .stream()
              .map(WeightedClientTransportSupplier::getBroker)
              .collect(Collectors.toSet());

      Set<Broker> remove = new HashSet<>(existingBrokers);
      remove.removeAll(incomingBrokers);
      Set<Broker> add = new HashSet<>(incomingBrokers);
      add.removeAll(existingBrokers);

      for (Broker broker : remove) {
        handleJoinEvent(broker);
      }

      for (Broker broker : add) {
        handleLeaveEvent(broker);
      }
    }
  }

  private Flux<Event> listenToBrokerEvents() {
    return Flux.defer(
            () ->
                client
                    .brokers(Empty.getDefaultInstance())
                    .collect(Collectors.toSet())
                    .doOnNext(this::reconcileSuppliers)
                    .thenMany(client.streamBrokerEvents(Empty.getDefaultInstance())))
        .doOnNext(this::handleBrokerEvent)
        .doOnError(
            t -> {
              logger.warn(
                  "error streaming broker events - make sure access key {} has a valid access token",
                  accessKey);
              logger.trace("error streaming broker events", t);
            })
        .onErrorResume(
            new Function<Throwable, Publisher<? extends Event>>() {
              long attempts = 0;
              long lastAttempt = System.currentTimeMillis();

              @Override
              public synchronized Publisher<? extends Event> apply(Throwable throwable) {
                if (Duration.ofMillis(System.currentTimeMillis() - lastAttempt).getSeconds() > 30) {
                  attempts = 0;
                }

                Mono<Event> then =
                    Mono.delay(Duration.ofMillis(attempts * 500)).then(Mono.error(throwable));
                if (attempts < 30) {
                  attempts++;
                }

                lastAttempt = System.currentTimeMillis();

                return then;
              }
            })
        .retry();
  }

  private void seedClientTransportSupplier() {
    synchronized (this) {
      missed++;
    }
    seedAddresses
        .stream()
        .map(
            address -> {
              try {
                Broker b;
                String str = address.toString();
                URI u = URI.create(str);
                switch (u.getScheme()) {
                  case "ws":
                  case "wss":
                    b =
                        Broker.newBuilder()
                            .setWebSocketAddress(u.getHost())
                            .setWebSocketPort(u.getPort())
                            .build();
                    return new WeightedClientTransportSupplier(
                        b, BrokerAddressSelectors.WEBSOCKET_ADDRESS, clientTransportFactory);
                  case "tcp":
                    b =
                        Broker.newBuilder()
                            .setTcpAddress(u.getHost())
                            .setTcpPort(u.getPort())
                            .build();
                    return new WeightedClientTransportSupplier(
                        b, BrokerAddressSelectors.TCP_ADDRESS, clientTransportFactory);
                  default:
                    // Assume URI is actually a HostAndPort, and TCP is our default
                    HostAndPort hostAndPort = HostAndPort.fromString(str);
                    b =
                        Broker.newBuilder()
                            .setTcpAddress(hostAndPort.getHost())
                            .setTcpPort(hostAndPort.getPort())
                            .build();
                    return new WeightedClientTransportSupplier(
                        b, BrokerAddressSelectors.TCP_ADDRESS, clientTransportFactory);
                }
              } catch (Throwable t) {
                InetSocketAddress address1 = (InetSocketAddress) address;

                logger.info("can't parse socket to URI");
                return new WeightedClientTransportSupplier(
                    Broker.newBuilder()
                        .setTcpAddress(address1.getHostName())
                        .setTcpPort(address1.getPort())
                        .build(),
                    BrokerAddressSelectors.TCP_ADDRESS,
                    clientTransportFactory);
              }
            })
        .forEach(suppliers::add);
  }

  private synchronized void handleBrokerEvent(final Event event) {
    logger.info("received broker event {} - {}", event.getType(), event.toString());
    final Broker broker = event.getBroker();
    switch (event.getType()) {
      case JOIN:
        handleJoinEvent(broker);
        break;
      case LEAVE:
        handleLeaveEvent(broker);
        break;
      default:
        throw new IllegalStateException("unknown event type " + event.getType());
    }
  }

  private void handleJoinEvent(final Broker broker) {
    final Id incomingBrokerId = broker.getBrokerId();
    final Optional<WeightedClientTransportSupplier> first =
        suppliers
            .stream()
            .filter(
                supplier -> Objects.equals(supplier.getBroker().getBrokerId(), incomingBrokerId))
            .findAny();

    if (!first.isPresent()) {
      logger.info("adding transport supplier to broker {}", broker);

      final WeightedClientTransportSupplier s =
          new WeightedClientTransportSupplier(broker, addressSelector, clientTransportFactory);
      suppliers.add(s);

      s.onClose()
          .doFinally(
              signalType -> {
                logger.info("removing transport supplier to broker {}", broker);
                suppliers.removeIf(
                    supplier -> supplier.getBroker().getBrokerId().equals(broker.getBrokerId()));
              })
          .subscribe();

      missed++;
      createConnection();
    }
  }

  private void handleLeaveEvent(final Broker broker) {
    suppliers
        .stream()
        .filter(
            supplier -> Objects.equals(supplier.getBroker().getBrokerId(), broker.getBrokerId()))
        .findAny()
        .ifPresent(
            supplier -> {
              logger.info("removing transport supplier to {}", broker);
              supplier.dispose();
              missed++;
            });
  }

  private WeightedReconnectingRSocket createWeightedReconnectingRSocket() {
    return WeightedReconnectingRSocket.newInstance(
        requestHandlingRSocket,
        createSetupPayloadSupplier(String.valueOf(poolCount++)),
        this::isDisposed,
        this::selectClientTransportSupplier,
        keepalive,
        tickPeriodSeconds,
        ackTimeoutSeconds,
        missedAcks,
        accessKey,
        accessToken,
        lowerQuantile,
        higherQuantile,
        INACTIVITY_FACTOR);
  }

  @Override
  public void dispose() {
    for (ByteBuf metadata : setupMetadata) {
      ReferenceCountUtil.safeRelease(metadata);
    }
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  private synchronized void createConnection() {
    if (members.size() < poolSize) {
      missed++;
      WeightedReconnectingRSocket rSocket = createWeightedReconnectingRSocket();
      members.add(rSocket);
    }
  }

  private boolean attemptInitialConnection = true;

  public RSocket selectRSocket() {
    RSocket rSocket;
    List<WeightedReconnectingRSocket> _m;
    int r;
    for (; ; ) {
      final int size;
      synchronized (this) {
        r = missed;
        _m = members;

        int _s = _m.size();

        boolean _a = false;
        if (attemptInitialConnection && _s == 0) {
          attemptInitialConnection = false;
          _a = true;
        }

        size =
            _a
                    || (_s < poolSize
                        && (System.currentTimeMillis() - selectRefreshTimeout)
                            > selectRefreshTimeoutDuration)
                ? -1
                : _s;
      }

      switch (size) {
        case -1:
          createConnection();
          continue;
        case 1:
          rSocket = _m.get(0);
          break;
        case 2:
          {
            WeightedReconnectingRSocket rsc1 = _m.get(0);
            WeightedReconnectingRSocket rsc2 = _m.get(1);

            double w1 = algorithmicWeight(rsc1, lowerQuantile, higherQuantile);
            double w2 = algorithmicWeight(rsc2, lowerQuantile, higherQuantile);
            if (w1 < w2) {
              rSocket = rsc2;
            } else {
              rSocket = rsc1;
            }
          }
          break;
        default:
          {
            WeightedReconnectingRSocket rsc1 = null;
            WeightedReconnectingRSocket rsc2 = null;

            for (int i = 0; i < EFFORT; i++) {
              int i1 = ThreadLocalRandom.current().nextInt(size);
              int i2 = ThreadLocalRandom.current().nextInt(size - 1);

              if (i2 >= i1) {
                i2++;
              }
              rsc1 = _m.get(i1);
              rsc2 = _m.get(i2);
              if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
                break;
              }
            }

            double w1 = algorithmicWeight(rsc1, lowerQuantile, higherQuantile);
            double w2 = algorithmicWeight(rsc2, lowerQuantile, higherQuantile);
            if (w1 < w2) {
              rSocket = rsc2;
            } else {
              rSocket = rsc1;
            }
          }
      }

      synchronized (this) {
        if (r == missed) {
          break;
        }
      }
    }

    return rSocket;
  }

  private static double algorithmicWeight(
      final WeightedRSocket socket, final Quantile lowerQuantile, final Quantile higherQuantile) {
    if (socket == null || socket.availability() == 0.0) {
      return 0.0;
    }
    final int pendings = socket.pending();
    double latency = socket.predictedLatency();

    final double low = lowerQuantile.estimation();
    final double high =
        Math.max(
            higherQuantile.estimation(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    final double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      latency /= calculateFactor(low, latency, bandWidth);
    } else if (latency > high) {
      latency *= calculateFactor(latency, high, bandWidth);
    }

    return socket.availability() * 1.0 / (1.0 + latency * (pendings + 1));
  }

  private static double calculateFactor(final double u, final double l, final double bandWidth) {
    final double alpha = (u - l) / bandWidth;
    return Math.pow(1 + alpha, EXP_FACTOR);
  }

  private WeightedClientTransportSupplier selectClientTransportSupplier() {
    WeightedClientTransportSupplier supplier;
    int c;
    for (; ; ) {
      final boolean selectTransports;
      final List<WeightedClientTransportSupplier> _s;
      synchronized (this) {
        c = missed;
        _s = suppliers;

        selectTransports = suppliers.isEmpty();
      }

      if (selectTransports) {
        seedClientTransportSupplier();
        continue;
      }

      final int size = _s.size();
      if (size == 1) {
        supplier = _s.get(0);
      } else {
        WeightedClientTransportSupplier supplier1;
        WeightedClientTransportSupplier supplier2;

        int i1 = ThreadLocalRandom.current().nextInt(size);
        int i2 = ThreadLocalRandom.current().nextInt(size - 1);

        if (i2 >= i1) {
          i2++;
        }

        supplier1 = _s.get(i1);
        supplier2 = _s.get(i2);

        double w1 = supplier1.weight();
        double w2 = supplier2.weight();

        if (logger.isDebugEnabled()) {
          logger.debug("selecting candidate socket {} with weight {}", supplier1.toString(), w1);
          logger.debug("selecting candidate socket {} with weight {}", supplier2.toString(), w2);
        }

        supplier = w1 < w2 ? supplier1 : supplier2;
      }

      synchronized (this) {
        if (c == missed) {
          supplier.select();
          missed++;
          break;
        }
      }
    }

    logger.info("selected socket {}", supplier.toString());

    return supplier;
  }
}
