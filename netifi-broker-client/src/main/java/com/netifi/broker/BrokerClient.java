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

import com.netifi.broker.discovery.DiscoveryStrategy;
import com.netifi.broker.frames.DestinationSetupFlyweight;
import com.netifi.broker.info.Broker;
import com.netifi.broker.rsocket.BrokerSocket;
import com.netifi.broker.rsocket.NamedRSocketClientWrapper;
import com.netifi.broker.rsocket.NamedRSocketServiceWrapper;
import com.netifi.broker.rsocket.transport.BrokerAddressSelectors;
import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentracing.Tracer;
import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.rpc.RSocketRpcService;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.netty.tcp.TcpClient;

/** This is where the magic happens */
public class BrokerClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(BrokerClient.class);
  private static final ConcurrentHashMap<String, BrokerClient> BROKERCLIENT =
      new ConcurrentHashMap<>();
  private static final String DEFAULT_DESTINATION = defaultDestination();

  static {
    // Set the Java DNS cache to 60 seconds
    java.security.Security.setProperty("networkaddress.cache.ttl", "60");
  }

  private final long accesskey;
  private final String group;
  private final String destination;
  private final Tags tags;
  private final BrokerService brokerService;
  private MonoProcessor<Void> onClose;
  private RequestHandlingRSocket requestHandlingRSocket;

  private BrokerClient(
      long accessKey,
      ByteBuf accessToken,
      String connectionIdSeed,
      InetAddress inetAddress,
      String group,
      String destination,
      short additionalFlags,
      Tags tags,
      boolean keepalive,
      long tickPeriodSeconds,
      long ackTimeoutSeconds,
      int missedAcks,
      List<SocketAddress> seedAddresses,
      Function<Broker, InetSocketAddress> addressSelector,
      Function<SocketAddress, ClientTransport> clientTransportFactory,
      RequestHandlingRSocket responder,
      boolean responderRequiresUnwrapping,
      int poolSize,
      Supplier<Tracer> tracerSupplier,
      DiscoveryStrategy discoveryStrategy) {
    this.accesskey = accessKey;
    this.group = group;
    this.destination = destination;
    this.tags = tags;
    this.onClose = MonoProcessor.create();
    this.requestHandlingRSocket = responder;
    this.brokerService =
        new DefaultBrokerService(
            seedAddresses,
            requestHandlingRSocket,
            responderRequiresUnwrapping,
            inetAddress,
            group,
            addressSelector,
            clientTransportFactory,
            poolSize,
            keepalive,
            tickPeriodSeconds,
            ackTimeoutSeconds,
            missedAcks,
            accessKey,
            accessToken,
            connectionIdSeed,
            additionalFlags,
            tags,
            tracerSupplier.get(),
            discoveryStrategy);
  }

  public String getGroup() {
    return group;
  }

  public String getDestination() {
    return destination;
  }

  @Deprecated
  public static Builder builder() {
    return new Builder();
  }

  public static WebSocketBuilder ws() {
    return new WebSocketBuilder();
  }

  public static TcpBuilder tcp() {
    return new TcpBuilder();
  }

  public static CustomizableBuilder customizable() {
    return new CustomizableBuilder();
  }

  private static String defaultDestination() {
    return UUID.randomUUID().toString();
  }

  @Override
  public void dispose() {
    requestHandlingRSocket.dispose();
    onClose.onComplete();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isTerminated();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  /**
   * Adds an RSocketRpcService to be handle requests
   *
   * @param service the RSocketRpcService instance
   * @return current BrokerClient builder instance
   */
  public BrokerClient addService(RSocketRpcService service) {
    Objects.requireNonNull(service);
    requestHandlingRSocket.addService(service);
    return this;
  }

  /**
   * Adds an RSocket handler that will be located by name. This lets BrokerClient bridge raw RSocket
   * betweens services that don't use RSocketRpcService. It will route to a RSocket by specific
   * name, but it will give you a raw data so the implementor must deal with the incoming Payload.
   *
   * @param name the name of the RSocket
   * @param rSocket the RSocket to handle the requests
   * @return current BrokerClient builder instance
   */
  public BrokerClient addNamedRSocket(String name, RSocket rSocket) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(rSocket);
    return addService(NamedRSocketServiceWrapper.wrap(name, rSocket));
  }

  @Deprecated
  public BrokerSocket destination(String destination, String group) {
    return groupServiceSocket(group, Tags.of("com.netifi.destination", destination));
  }

  @Deprecated
  public BrokerSocket group(String group) {
    return groupServiceSocket(group, Tags.empty());
  }

  @Deprecated
  public BrokerSocket broadcast(String group) {
    return broadcastServiceSocket(group, Tags.empty());
  }

  @Deprecated
  public BrokerSocket shard(String group, ByteBuf shardKey) {
    return shardServiceSocket(group, shardKey, Tags.empty());
  }

  public BrokerSocket groupServiceSocket(String group) {
    return groupServiceSocket(group, Tags.empty());
  }

  public BrokerSocket groupServiceSocket(String group, Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);
    return brokerService.group(group, tags);
  }

  public BrokerSocket broadcastServiceSocket(String group) {
    return broadcastServiceSocket(group, Tags.empty());
  }

  public BrokerSocket broadcastServiceSocket(String group, Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);
    return brokerService.broadcast(group, tags);
  }

  public BrokerSocket shardServiceSocket(String group, ByteBuf shardKey) {
    return shardServiceSocket(group, shardKey, Tags.empty());
  }

  public BrokerSocket shardServiceSocket(String group, ByteBuf shardKey, Tags tags) {
    Objects.requireNonNull(group);
    Objects.requireNonNull(tags);
    return brokerService.shard(group, shardKey, tags);
  }

  public BrokerSocket groupNamedRSocket(String name, String group) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), groupServiceSocket(group, Tags.empty()));
  }

  public BrokerSocket groupNamedRSocket(String name, String group, Tags tags) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), groupServiceSocket(group, tags));
  }

  public BrokerSocket broadcastNamedRSocket(String name, String group) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), broadcastServiceSocket(group, Tags.empty()));
  }

  public BrokerSocket broadcastNamedRSocket(String name, String group, Tags tags) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), broadcastServiceSocket(group, tags));
  }

  public BrokerSocket shardNamedRSocket(String name, String group, ByteBuf shardKey) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), shardServiceSocket(group, shardKey, Tags.empty()));
  }

  public BrokerSocket shardNamedRSocket(String name, String group, ByteBuf shardKey, Tags tags) {
    return NamedRSocketClientWrapper.wrap(
        Objects.requireNonNull(name), shardServiceSocket(group, shardKey, tags));
  }

  /**
   * This is an advanced API that lets you select a raw {@link RSocket} to the broker. Do not use
   * this unless you know what you are doing. It will not provide any routing metadata, or wrapping
   *
   * @return a raw RSocket
   */
  public RSocket selectRSocket() {
    return brokerService.selectRSocket();
  }

  public long getAccesskey() {
    return accesskey;
  }

  public String getGroupName() {
    return group;
  }

  public Tags getTags() {
    return tags;
  }

  public abstract static class CommonBuilder<SELF extends CommonBuilder<SELF>> {
    Long accessKey = DefaultBuilderConfig.getAccessKey();
    String group = DefaultBuilderConfig.getGroup();
    String destination = DefaultBuilderConfig.getDestination();
    short additionalFlags = DefaultBuilderConfig.getAdditionalConnectionFlags();
    Tags tags = DefaultBuilderConfig.getTags();
    String accessToken = DefaultBuilderConfig.getAccessToken();
    byte[] accessTokenBytes = new byte[20];
    String connectionIdSeed = DefaultBuilderConfig.getConnectionId();
    int poolSize = Runtime.getRuntime().availableProcessors() * 2;
    Supplier<Tracer> tracerSupplier = () -> null;
    boolean keepalive = DefaultBuilderConfig.getKeepAlive();
    long tickPeriodSeconds = DefaultBuilderConfig.getTickPeriodSeconds();
    long ackTimeoutSeconds = DefaultBuilderConfig.getAckTimeoutSeconds();
    int missedAcks = DefaultBuilderConfig.getMissedAcks();
    InetAddress inetAddress = DefaultBuilderConfig.getLocalAddress();
    String host = DefaultBuilderConfig.getHost();
    Integer port = DefaultBuilderConfig.getPort();
    List<SocketAddress> seedAddresses = DefaultBuilderConfig.getSeedAddress();
    String netifiKey;
    List<SocketAddress> socketAddresses;
    DiscoveryStrategy discoveryStrategy = null;
    RequestHandlingRSocket responder = new RequestHandlingRSocket(); // DEFAULT
    boolean responderRequiresUnwrapping = true; // DEFAULT

    public SELF discoveryStrategy(DiscoveryStrategy discoveryStrategy) {
      this.discoveryStrategy = discoveryStrategy;
      return (SELF) this;
    }

    public SELF isPublic(boolean enablePublicAccess) {
      if (enablePublicAccess) {
        additionalFlags |= DestinationSetupFlyweight.FLAG_ENABLE_PUBLIC_ACCESS;
      } else {
        additionalFlags &= ~DestinationSetupFlyweight.FLAG_ENABLE_PUBLIC_ACCESS;
      }

      return (SELF) this;
    }

    public SELF jwt(String jwtToken) {
      this.accessToken = jwtToken;
      this.accessKey = DestinationSetupFlyweight.JWT_AUTHENTICATION;
      additionalFlags |= DestinationSetupFlyweight.FLAG_ALTERNATIVE_AUTHENTICATION;
      return (SELF) this;
    }

    public SELF poolSize(int poolSize) {
      this.poolSize = poolSize;
      return (SELF) this;
    }

    public SELF tracerSupplier(Supplier<Tracer> tracerSupplier) {
      this.tracerSupplier = tracerSupplier;
      return (SELF) this;
    }

    public SELF accessKey(long accessKey) {
      this.accessKey = accessKey;
      return (SELF) this;
    }

    public SELF accessToken(String accessToken) {
      this.accessToken = accessToken;
      return (SELF) this;
    }

    public SELF connectionId(String connectionId) {
      this.connectionIdSeed = connectionId;
      return (SELF) this;
    }

    public SELF additionalConnectionFlags(short flags) {
      this.additionalFlags = flags;
      return (SELF) this;
    }

    public SELF group(String group) {
      this.group = group;
      return (SELF) this;
    }

    public SELF destination(String destination) {
      this.destination = destination;
      return (SELF) this;
    }

    public SELF tag(String key, String value) {
      this.tags = tags.and(key, value);
      return (SELF) this;
    }

    public SELF tags(String... tags) {
      this.tags = this.tags.and(tags);
      return (SELF) this;
    }

    public SELF tags(Iterable<Tag> tags) {
      this.tags = this.tags.and(tags);
      return (SELF) this;
    }

    public SELF keepalive(boolean useKeepAlive) {
      this.keepalive = useKeepAlive;
      return (SELF) this;
    }

    public SELF tickPeriodSeconds(long tickPeriodSeconds) {
      this.tickPeriodSeconds = tickPeriodSeconds;
      return (SELF) this;
    }

    public SELF ackTimeoutSeconds(long ackTimeoutSeconds) {
      this.ackTimeoutSeconds = ackTimeoutSeconds;
      return (SELF) this;
    }

    public SELF missedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      return (SELF) this;
    }

    public SELF host(String host) {
      this.host = host;
      return (SELF) this;
    }

    public SELF port(int port) {
      this.port = port;
      return (SELF) this;
    }

    public SELF seedAddresses(Collection<SocketAddress> addresses) {
      if (addresses instanceof List) {
        this.seedAddresses = (List<SocketAddress>) addresses;
      } else {
        this.seedAddresses = new ArrayList<>(addresses);
      }

      return (SELF) this;
    }

    /**
     * Lets you add a strings in the form host:port
     *
     * @param address the first address to seed the broker with.
     * @param addresses additional addresses to seed the broker with.
     * @return the initial builder.
     */
    public SELF seedAddresses(String address, String... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(toInetSocketAddress(address));

      if (addresses != null) {
        for (String s : addresses) {
          list.add(toInetSocketAddress(address));
        }
      }

      return seedAddresses(list);
    }

    public SELF seedAddresses(SocketAddress address, SocketAddress... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(address);

      if (addresses != null) {
        list.addAll(Arrays.asList(addresses));
      }

      return seedAddresses(list);
    }

    public SELF localAddress(String address) {
      try {
        return localAddress(InetAddress.getByName(address));
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }

    public SELF localAddress(InetAddress address) {
      this.inetAddress = address;
      return (SELF) this;
    }

    public SELF requestHandler(RequestHandlingRSocket responder, boolean requiresUnwrapping) {
      this.responder = responder;
      this.responderRequiresUnwrapping = requiresUnwrapping;
      return (SELF) this;
    }

    private InetSocketAddress toInetSocketAddress(String address) {
      Objects.requireNonNull(address);
      String[] s = address.split(":");

      if (s.length != 2) {
        throw new IllegalArgumentException(address + " was a valid host address");
      }

      return InetSocketAddress.createUnresolved(s[0], Integer.parseInt(s[1]));
    }

    void prebuild() {
      Objects.requireNonNull(accessKey, "account key is required");
      Objects.requireNonNull(accessToken, "account token is required");
      Objects.requireNonNull(group, "group is required");

      if ((additionalFlags & DestinationSetupFlyweight.FLAG_ALTERNATIVE_AUTHENTICATION)
          == DestinationSetupFlyweight.FLAG_ALTERNATIVE_AUTHENTICATION) {
        if (accessKey == DestinationSetupFlyweight.JWT_AUTHENTICATION) {
          logger.debug("using JWT authentication");
          this.accessTokenBytes = accessToken.getBytes(StandardCharsets.UTF_8);
        } else {
          throw new IllegalStateException("unknown alternative authentication type: " + accessKey);
        }
      } else {
        logger.debug("using access key and token authentication");
        this.accessTokenBytes = Base64.getDecoder().decode(accessToken);
      }

      if (destination == null) {
        destination = DEFAULT_DESTINATION;
      }
      tags = tags.and("com.netifi.destination", destination);

      this.connectionIdSeed =
          this.connectionIdSeed == null ? UUID.randomUUID().toString() : this.connectionIdSeed;

      if (inetAddress == null) {
        try {
          inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          inetAddress = InetAddress.getLoopbackAddress();
        }
      }

      if (discoveryStrategy == null) {
        if (seedAddresses == null) {
          Objects.requireNonNull(host, "host is required");
          Objects.requireNonNull(port, "port is required");
          socketAddresses =
              Collections.singletonList(InetSocketAddress.createUnresolved(host, port));
        } else {
          socketAddresses = seedAddresses;
        }
      }

      logger.info("registering with netifi with group {}", group);

      netifiKey = accessKey + group + tags.toString();
    }
  }

  public static class WebSocketBuilder extends CommonBuilder<WebSocketBuilder> {
    private boolean sslDisabled = DefaultBuilderConfig.isSslDisabled();
    private Callable<SslContext> sslContextSupplier =
        () -> {
          final SslProvider sslProvider;
          if (OpenSsl.isAvailable()) {
            logger.info("Native SSL provider is available; will use native provider.");
            sslProvider = SslProvider.OPENSSL_REFCNT;
          } else {
            logger.info("Native SSL provider not available; will use JDK SSL provider.");
            sslProvider = SslProvider.JDK;
          }
          return SslContextBuilder.forClient()
              .trustManager(InsecureTrustManagerFactory.INSTANCE)
              .sslProvider(sslProvider)
              .build();
        };

    public WebSocketBuilder disableSsl() {
      this.sslDisabled = true;
      return this;
    }

    public WebSocketBuilder enableSsl() {
      this.sslDisabled = false;
      return this;
    }

    public WebSocketBuilder enableSsl(Callable<SslContext> sslContextSupplier) {
      this.sslContextSupplier = sslContextSupplier;
      return enableSsl();
    }

    public BrokerClient build() {
      prebuild();

      Function<SocketAddress, ClientTransport> clientTransportFactory;

      logger.info("BrokerClient transport factory not provided; using WS transport.");
      if (sslDisabled) {
        clientTransportFactory =
            address -> {
              TcpClient client = TcpClient.create().addressSupplier(() -> address);
              return WebsocketClientTransport.create(client);
            };
      } else {
        try {
          final SslContext sslContext = sslContextSupplier.call();
          clientTransportFactory =
              address -> {
                TcpClient client =
                    TcpClient.create().addressSupplier(() -> address).secure(sslContext);
                return WebsocketClientTransport.create(client);
              };
        } catch (Exception sslException) {
          throw Exceptions.propagate(sslException);
        }
      }

      return BROKERCLIENT.computeIfAbsent(
          netifiKey,
          _k -> {
            BrokerClient brokerClient =
                new BrokerClient(
                    accessKey,
                    Unpooled.wrappedBuffer(accessTokenBytes),
                    connectionIdSeed,
                    inetAddress,
                    group,
                    destination,
                    additionalFlags,
                    tags,
                    keepalive,
                    tickPeriodSeconds,
                    ackTimeoutSeconds,
                    missedAcks,
                    socketAddresses,
                    BrokerAddressSelectors.WEBSOCKET_ADDRESS,
                    clientTransportFactory,
                    responder,
                    responderRequiresUnwrapping,
                    poolSize,
                    tracerSupplier,
                    discoveryStrategy);
            brokerClient.onClose.doFinally(s -> BROKERCLIENT.remove(netifiKey)).subscribe();

            return brokerClient;
          });
    }
  }

  public static class TcpBuilder extends CommonBuilder<TcpBuilder> {
    private boolean sslDisabled = DefaultBuilderConfig.isSslDisabled();
    private Callable<SslContext> sslContextSupplier =
        () -> {
          final SslProvider sslProvider;
          if (OpenSsl.isAvailable()) {
            logger.info("Native SSL provider is available; will use native provider.");
            sslProvider = SslProvider.OPENSSL_REFCNT;
          } else {
            logger.info("Native SSL provider not available; will use JDK SSL provider.");
            sslProvider = SslProvider.JDK;
          }
          return SslContextBuilder.forClient()
              .trustManager(InsecureTrustManagerFactory.INSTANCE)
              .sslProvider(sslProvider)
              .build();
        };

    public TcpBuilder disableSsl() {
      this.sslDisabled = true;
      return this;
    }

    public TcpBuilder enableSsl() {
      this.sslDisabled = false;
      return this;
    }

    public TcpBuilder enableSsl(Callable<SslContext> sslContextSupplier) {
      this.sslContextSupplier = sslContextSupplier;
      return enableSsl();
    }

    public BrokerClient build() {
      prebuild();

      Function<SocketAddress, ClientTransport> clientTransportFactory;

      logger.info("BrokerClient transport factory not provided; using WS transport.");
      if (sslDisabled) {
        clientTransportFactory =
            address -> {
              TcpClient client = TcpClient.create().addressSupplier(() -> address);
              return TcpClientTransport.create(client);
            };
      } else {
        try {
          final SslContext sslContext = sslContextSupplier.call();
          clientTransportFactory =
              address -> {
                TcpClient client =
                    TcpClient.create().addressSupplier(() -> address).secure(sslContext);
                return TcpClientTransport.create(client);
              };
        } catch (Exception sslException) {
          throw Exceptions.propagate(sslException);
        }
      }

      return BROKERCLIENT.computeIfAbsent(
          netifiKey,
          _k -> {
            BrokerClient brokerClient =
                new BrokerClient(
                    accessKey,
                    Unpooled.wrappedBuffer(accessTokenBytes),
                    connectionIdSeed,
                    inetAddress,
                    group,
                    destination,
                    additionalFlags,
                    tags,
                    keepalive,
                    tickPeriodSeconds,
                    ackTimeoutSeconds,
                    missedAcks,
                    socketAddresses,
                    BrokerAddressSelectors.TCP_ADDRESS,
                    clientTransportFactory,
                    responder,
                    responderRequiresUnwrapping,
                    poolSize,
                    tracerSupplier,
                    discoveryStrategy);
            brokerClient.onClose.doFinally(s -> BROKERCLIENT.remove(netifiKey)).subscribe();

            return brokerClient;
          });
    }
  }

  public static class CustomizableBuilder extends CommonBuilder<CustomizableBuilder> {
    Function<SocketAddress, ClientTransport> clientTransportFactory;
    Function<Broker, InetSocketAddress> addressSelector;

    public CustomizableBuilder clientTransportFactory(
        Function<SocketAddress, ClientTransport> clientTransportFactory) {
      this.clientTransportFactory = clientTransportFactory;
      return this;
    }

    public CustomizableBuilder addressSelector(
        Function<Broker, InetSocketAddress> addressSelector) {
      this.addressSelector = addressSelector;
      return this;
    }

    public BrokerClient build() {
      prebuild();

      return BROKERCLIENT.computeIfAbsent(
          netifiKey,
          _k -> {
            BrokerClient brokerClient =
                new BrokerClient(
                    accessKey,
                    Unpooled.wrappedBuffer(accessTokenBytes),
                    connectionIdSeed,
                    inetAddress,
                    group,
                    destination,
                    additionalFlags,
                    tags,
                    keepalive,
                    tickPeriodSeconds,
                    ackTimeoutSeconds,
                    missedAcks,
                    socketAddresses,
                    addressSelector,
                    clientTransportFactory,
                    responder,
                    responderRequiresUnwrapping,
                    poolSize,
                    tracerSupplier,
                    discoveryStrategy);
            brokerClient.onClose.doFinally(s -> BROKERCLIENT.remove(netifiKey)).subscribe();

            return brokerClient;
          });
    }
  }

  @Deprecated
  public static class Builder {
    private InetAddress inetAddress = DefaultBuilderConfig.getLocalAddress();
    private String host = DefaultBuilderConfig.getHost();
    private Integer port = DefaultBuilderConfig.getPort();
    private List<SocketAddress> seedAddresses = DefaultBuilderConfig.getSeedAddress();
    private Long accessKey = DefaultBuilderConfig.getAccessKey();
    private String group = DefaultBuilderConfig.getGroup();
    private String destination = DefaultBuilderConfig.getDestination();
    private Tags tags = DefaultBuilderConfig.getTags();
    private String accessToken = DefaultBuilderConfig.getAccessToken();
    private byte[] accessTokenBytes = new byte[20];
    private String connectionIdSeed = initialConnectionId();
    private boolean sslDisabled = DefaultBuilderConfig.isSslDisabled();
    private boolean keepalive = DefaultBuilderConfig.getKeepAlive();
    private long tickPeriodSeconds = DefaultBuilderConfig.getTickPeriodSeconds();
    private long ackTimeoutSeconds = DefaultBuilderConfig.getAckTimeoutSeconds();
    private int missedAcks = DefaultBuilderConfig.getMissedAcks();
    private DiscoveryStrategy discoveryStrategy = null;
    private Function<Broker, InetSocketAddress> addressSelector =
        BrokerAddressSelectors.TCP_ADDRESS; // Default

    private Function<SocketAddress, ClientTransport> clientTransportFactory = null;
    private int poolSize = Runtime.getRuntime().availableProcessors() * 2;
    private Supplier<Tracer> tracerSupplier = () -> null;

    public static Builder fromCustomizableBuilder(CustomizableBuilder customizableBuilder) {
      Builder builder = new Builder();

      builder.clientTransportFactory = customizableBuilder.clientTransportFactory;
      builder.accessKey = customizableBuilder.accessKey;
      builder.accessToken = customizableBuilder.accessToken;
      builder.accessTokenBytes = customizableBuilder.accessTokenBytes;
      builder.ackTimeoutSeconds = customizableBuilder.ackTimeoutSeconds;
      builder.addressSelector = customizableBuilder.addressSelector;
      builder.connectionIdSeed = customizableBuilder.connectionIdSeed;
      builder.destination = customizableBuilder.destination;
      builder.discoveryStrategy = customizableBuilder.discoveryStrategy;
      builder.group = customizableBuilder.group;
      builder.host = customizableBuilder.host;
      builder.inetAddress = customizableBuilder.inetAddress;
      builder.missedAcks = customizableBuilder.missedAcks;
      builder.poolSize = customizableBuilder.poolSize;
      builder.port = customizableBuilder.port;
      builder.seedAddresses = customizableBuilder.seedAddresses;
      builder.tags = customizableBuilder.tags;
      builder.tickPeriodSeconds = customizableBuilder.tickPeriodSeconds;
      builder.tracerSupplier = customizableBuilder.tracerSupplier;

      return builder;
    }

    private static String initialConnectionId() {
      return UUID.randomUUID().toString();
    }

    public Builder clientTransportFactory(
        Function<SocketAddress, ClientTransport> clientTransportFactory) {
      this.clientTransportFactory = clientTransportFactory;
      return this;
    }

    public Builder poolSize(int poolSize) {
      this.poolSize = poolSize;
      return this;
    }

    public Builder sslDisabled(boolean sslDisabled) {
      this.sslDisabled = sslDisabled;
      return this;
    }

    public Builder keepalive(boolean useKeepAlive) {
      this.keepalive = useKeepAlive;
      return this;
    }

    public Builder tickPeriodSeconds(long tickPeriodSeconds) {
      this.tickPeriodSeconds = tickPeriodSeconds;
      return this;
    }

    public Builder ackTimeoutSeconds(long ackTimeoutSeconds) {
      this.ackTimeoutSeconds = ackTimeoutSeconds;
      return this;
    }

    public Builder missedAcks(int missedAcks) {
      this.missedAcks = missedAcks;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder tracerSupplier(Supplier<Tracer> tracerSupplier) {
      this.tracerSupplier = tracerSupplier;
      return this;
    }

    public Builder discoveryStrategy(DiscoveryStrategy discoveryStrategy) {
      this.discoveryStrategy = discoveryStrategy;
      return this;
    }

    public Builder seedAddresses(Collection<SocketAddress> addresses) {
      if (addresses instanceof List) {
        this.seedAddresses = (List<SocketAddress>) addresses;
      } else {
        this.seedAddresses = new ArrayList<>(addresses);
      }

      return this;
    }

    private InetSocketAddress toInetSocketAddress(String address) {
      Objects.requireNonNull(address);
      String[] s = address.split(":");

      if (s.length != 2) {
        throw new IllegalArgumentException(address + " was a valid host address");
      }

      return InetSocketAddress.createUnresolved(s[0], Integer.parseInt(s[1]));
    }

    /**
     * Lets you add a strings in the form host:port
     *
     * @param address the first address to seed the broker with.
     * @param addresses additional addresses to seed the broker with.
     * @return the initial builder.
     */
    public Builder seedAddresses(String address, String... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(toInetSocketAddress(address));

      if (addresses != null) {
        for (String s : addresses) {
          list.add(toInetSocketAddress(address));
        }
      }

      return seedAddresses(list);
    }

    public Builder seedAddresses(SocketAddress address, SocketAddress... addresses) {
      List<SocketAddress> list = new ArrayList<>();
      list.add(address);

      if (addresses != null) {
        list.addAll(Arrays.asList(addresses));
      }

      return seedAddresses(list);
    }

    public Builder localAddress(String address) {
      try {
        return localAddress(InetAddress.getByName(address));
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }

    public Builder localAddress(InetAddress address) {
      this.inetAddress = address;
      return this;
    }

    public Builder accessKey(long accessKey) {
      this.accessKey = accessKey;
      return this;
    }

    public Builder accessToken(String accessToken) {
      this.accessToken = accessToken;
      return this;
    }

    public Builder group(String group) {
      this.group = group;
      return this;
    }

    public Builder destination(String destination) {
      this.destination = destination;
      return this;
    }

    public Builder tag(String key, String value) {
      this.tags = tags.and(key, value);
      return this;
    }

    public Builder tags(String... tags) {
      this.tags = this.tags.and(tags);
      return this;
    }

    public Builder tags(Iterable<Tag> tags) {
      this.tags = this.tags.and(tags);
      return this;
    }

    public Builder addressSelector(Function<Broker, InetSocketAddress> addressSelector) {
      this.addressSelector = addressSelector;
      return this;
    }

    public CustomizableBuilder toCustomizableBuilder() {
      CustomizableBuilder builder = new CustomizableBuilder();

      builder.clientTransportFactory = this.clientTransportFactory;
      builder.accessKey = this.accessKey;
      builder.accessToken = this.accessToken;
      builder.accessTokenBytes = this.accessTokenBytes;
      builder.ackTimeoutSeconds = this.ackTimeoutSeconds;
      builder.addressSelector = this.addressSelector;
      builder.connectionIdSeed = this.connectionIdSeed;
      builder.destination = this.destination;
      builder.discoveryStrategy = this.discoveryStrategy;
      builder.group = this.group;
      builder.host = this.host;
      builder.inetAddress = this.inetAddress;
      builder.missedAcks = this.missedAcks;
      builder.poolSize = this.poolSize;
      builder.port = this.port;
      builder.seedAddresses = this.seedAddresses;
      builder.tags = this.tags;
      builder.tickPeriodSeconds = this.tickPeriodSeconds;
      builder.tracerSupplier = this.tracerSupplier;

      return builder;
    }

    public BrokerClient build() {
      Objects.requireNonNull(accessKey, "account key is required");
      Objects.requireNonNull(accessToken, "account token is required");
      Objects.requireNonNull(group, "group is required");
      if (destination == null) {
        destination = DEFAULT_DESTINATION;
      }
      tags = tags.and("com.netifi.destination", destination);

      if (clientTransportFactory == null) {
        logger.info("BrokerClient transport factory not provided; using TCP transport.");
        if (sslDisabled) {
          clientTransportFactory =
              address -> {
                TcpClient client = TcpClient.create().addressSupplier(() -> address);
                return TcpClientTransport.create(client);
              };
        } else {
          try {
            final SslProvider sslProvider;
            if (OpenSsl.isAvailable()) {
              logger.info("Native SSL provider is available; will use native provider.");
              sslProvider = SslProvider.OPENSSL_REFCNT;
            } else {
              logger.info("Native SSL provider not available; will use JDK SSL provider.");
              sslProvider = SslProvider.JDK;
            }
            final SslContext sslContext =
                SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .sslProvider(sslProvider)
                    .build();
            clientTransportFactory =
                address -> {
                  TcpClient client =
                      TcpClient.create().addressSupplier(() -> address).secure(sslContext);
                  return TcpClientTransport.create(client);
                };
          } catch (Exception sslException) {
            throw Exceptions.bubble(sslException);
          }
        }
      }

      this.accessTokenBytes = Base64.getDecoder().decode(accessToken);

      if (inetAddress == null) {
        try {
          inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          inetAddress = InetAddress.getLoopbackAddress();
        }
      }

      List<SocketAddress> socketAddresses = null;
      if (discoveryStrategy == null) {
        if (seedAddresses == null) {
          Objects.requireNonNull(host, "host is required");
          Objects.requireNonNull(port, "port is required");
          socketAddresses =
              Collections.singletonList(InetSocketAddress.createUnresolved(host, port));
        } else {
          socketAddresses = seedAddresses;
        }
      }

      logger.info("registering with netifi with group {}", group);

      String netifiKey = accessKey + group;

      List<SocketAddress> _s = socketAddresses;
      return BROKERCLIENT.computeIfAbsent(
          netifiKey,
          _k -> {
            BrokerClient brokerClient =
                new BrokerClient(
                    accessKey,
                    Unpooled.wrappedBuffer(accessTokenBytes),
                    connectionIdSeed,
                    inetAddress,
                    group,
                    destination,
                    (short) 0,
                    tags,
                    keepalive,
                    tickPeriodSeconds,
                    ackTimeoutSeconds,
                    missedAcks,
                    _s,
                    addressSelector,
                    clientTransportFactory,
                    new RequestHandlingRSocket(),
                    true,
                    poolSize,
                    tracerSupplier,
                    discoveryStrategy);
            brokerClient.onClose.doFinally(s -> BROKERCLIENT.remove(netifiKey)).subscribe();

            return brokerClient;
          });
    }
  }
}
