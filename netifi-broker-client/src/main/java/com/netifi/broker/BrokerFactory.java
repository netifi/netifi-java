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
import com.netifi.broker.discovery.StaticListDiscoveryConfig;
import com.netifi.broker.discovery.StaticListDiscoveryStrategy;
import com.netifi.broker.frames.DestinationSetupFlyweight;
import com.netifi.broker.info.Broker;
import com.netifi.broker.rsocket.transport.BrokerAddressSelectors;
import com.netifi.common.net.HostAndPort;
import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import io.netty.buffer.Unpooled;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.CharsetUtil;
import io.opentracing.Tracer;
import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.ipc.MutableRouter;
import io.rsocket.ipc.RoutingServerRSocket;
import io.rsocket.ipc.routing.SimpleRouter;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;

public final class BrokerFactory {

  private static final Logger logger = LoggerFactory.getLogger(BrokerFactory.class);
  private static final ConcurrentHashMap<String, BrokerService> BROKERCLIENT =
      new ConcurrentHashMap<>();
  private static final String DEFAULT_DESTINATION = defaultDestination();

  private BrokerFactory() {}

  public static ClientBuilder connect() {
    return new ClientBuilder();
  }

  private static String defaultDestination() {
    return UUID.randomUUID().toString();
  }

  public interface KeepAliveConfig {
    Duration tickPeriod();

    Duration acknowledgeTimeout();

    int missedAcknowledges();

    static Builder builder() {
      return new Builder();
    }

    static KeepAliveConfig noKeepAlive() {
      return NoKeepAliveConfig.INSTANCE;
    }

    abstract class BaseBuilder {
      abstract KeepAliveConfig build();
    }

    class Builder extends BaseBuilder {
      Duration tickPeriod = Duration.ofSeconds(DefaultBuilderConfig.getTickPeriodSeconds());
      Duration acknowledgeTimeout = Duration.ofSeconds(DefaultBuilderConfig.getAckTimeoutSeconds());
      int missedAcknowledges = DefaultBuilderConfig.getMissedAcks();

      public Builder tickPeriod(Duration tickPeriod) {
        this.tickPeriod = tickPeriod;
        return this;
      }

      public Builder acknowledgeTimeout(Duration acknowledgeTimeout) {
        this.acknowledgeTimeout = acknowledgeTimeout;
        return this;
      }

      public Builder missedAcknowledges(int missedAcknowledges) {
        this.missedAcknowledges = missedAcknowledges;
        return this;
      }

      KeepAliveConfig build() {
        return new KeepAliveConfig() {
          @Override
          public Duration tickPeriod() {
            return tickPeriod;
          }

          @Override
          public Duration acknowledgeTimeout() {
            return acknowledgeTimeout;
          }

          @Override
          public int missedAcknowledges() {
            return missedAcknowledges;
          }
        };
      }
    }

    class NoKeepAliveBuilder extends BaseBuilder {

      @Override
      KeepAliveConfig build() {
        return NoKeepAliveConfig.INSTANCE;
      }
    }

    final class NoKeepAliveConfig implements KeepAliveConfig {
      static final NoKeepAliveConfig INSTANCE = new NoKeepAliveConfig();

      private NoKeepAliveConfig() {}

      @Override
      public Duration tickPeriod() {
        return Duration.ofMillis(-1);
      }

      @Override
      public Duration acknowledgeTimeout() {
        return Duration.ofMillis(-1);
      }

      @Override
      public int missedAcknowledges() {
        return -1;
      }
    }

    interface Spec {
      void noKeepAlive();

      Builder configure();
    }

    class DefaultSpec implements Spec {

      BaseBuilder builder;

      DefaultSpec() {
        if (DefaultBuilderConfig.getKeepAlive()) {
          configure();
        } else {
          noKeepAlive();
        }
      }

      @Override
      public void noKeepAlive() {
        builder = new NoKeepAliveBuilder();
      }

      @Override
      public Builder configure() {
        Builder builder = new Builder();
        this.builder = builder;
        return builder;
      }
    }
  }

  public interface SslConfig {

    Supplier<SslContext> sslContextProvider();

    static SslConfig custom(Supplier<SslContext> sslContextSupplier) {
      return () -> sslContextSupplier;
    }

    static NoSslConfig noSslConfig() {
      return NoSslConfig.INSTANCE;
    }

    static SslConfig defaultSslConfig() {
      return DefaultSslConfig.INSTANCE;
    }

    final class NoSslConfig implements SslConfig {
      static final NoSslConfig INSTANCE = new NoSslConfig();

      private NoSslConfig() {}

      @Override
      public Supplier<SslContext> sslContextProvider() {
        return null;
      }
    }

    final class DefaultSslConfig implements SslConfig {

      static final DefaultSslConfig INSTANCE = new DefaultSslConfig();

      private DefaultSslConfig() {}

      @Override
      public Supplier<SslContext> sslContextProvider() {
        return SUPPLIER_INSTANCE;
      }

      static final Supplier<SslContext> SUPPLIER_INSTANCE =
          () -> {
            final SslProvider sslProvider;
            if (OpenSsl.isAvailable()) {
              logger.info("Native SSL provider is available; will use native provider.");
              sslProvider = SslProvider.OPENSSL_REFCNT;
            } else {
              logger.info("Native SSL provider not available; will use JDK SSL provider.");
              sslProvider = SslProvider.JDK;
            }
            try {
              return SslContextBuilder.forClient()
                  .trustManager(InsecureTrustManagerFactory.INSTANCE)
                  .sslProvider(sslProvider)
                  .build();
            } catch (Throwable e) {
              throw Exceptions.propagate(e);
            }
          };
    }

    interface Spec {
      void unsecured();

      void secured();

      void secured(Supplier<SslContext> sslContextSupplier);
    }

    class DefaultSpec implements Spec {

      SslConfig sslConfig;

      DefaultSpec() {
        if (DefaultBuilderConfig.isSslDisabled()) {
          unsecured();
        } else {
          secured();
        }
      }

      @Override
      public void unsecured() {
        sslConfig = new NoSslConfig();
      }

      @Override
      public void secured() {
        sslConfig = new DefaultSslConfig();
      }

      @Override
      public void secured(Supplier<SslContext> sslContextSupplier) {
        sslConfig = () -> sslContextSupplier;
      }
    }
  }

  public interface DiscoveryConfig {

    DiscoveryStrategy discoveryStrategy();

    static DiscoveryConfig staticDiscovery(int port, String... hosts) {
      return () -> new StaticListDiscoveryStrategy(new StaticListDiscoveryConfig(port, hosts));
    }

    static DiscoveryConfig staticDiscovery(HostAndPort... addresses) {
      return () -> (DiscoveryStrategy) () -> Mono.just(Arrays.asList(addresses));
    }

    static DiscoveryConfig custom(DiscoveryStrategy discoveryStrategy) {
      return () -> discoveryStrategy;
    }

    interface Spec {
      void simple(int port, String... hosts);

      void simple(HostAndPort... addresses);

      void custom(DiscoveryStrategy discoveryStrategy);
    }

    class DefaultSpec implements Spec {

      DiscoveryConfig config;

      @Override
      public void simple(int port, String... hosts) {
        config = staticDiscovery(port, hosts);
      }

      @Override
      public void simple(HostAndPort... addresses) {
        config = staticDiscovery(addresses);
      }

      @Override
      public void custom(DiscoveryStrategy discoveryStrategy) {
        config = DiscoveryConfig.custom(discoveryStrategy);
      }
    }
  }

  public interface AuthenticationConfig {
    long accessKey();

    byte[] accessToken();

    static AuthenticationConfig simple(long accessKey, String bast64EncodedAccessToken) {
      return simple(accessKey, Base64.getDecoder().decode(bast64EncodedAccessToken));
    }

    static AuthenticationConfig simple(long accessKey, byte[] accessToken) {
      return new AuthenticationConfig() {
        @Override
        public long accessKey() {
          return accessKey;
        }

        @Override
        public byte[] accessToken() {
          return accessToken;
        }
      };
    }

    static AuthenticationConfig jwt(String jwtTokenString) {
      return new JwtAuthenticationConfig(jwtTokenString.getBytes(CharsetUtil.UTF_8));
    }

    static AuthenticationConfig jwt(byte[] jwtToken) {
      return new JwtAuthenticationConfig(jwtToken);
    }

    interface Spec {
      JwtSpec jwt();

      SimpleSpec simple();
    }

    interface SimpleSpec {
      SimpleSpec key(long accessKey);

      SimpleSpec token(String bast64EncodedAccessToken);

      SimpleSpec token(byte[] accessToken);
    }

    interface JwtSpec {

      JwtSpec token(String jwtTokenString);

      JwtSpec token(byte[] jwtToken);
    }

    class DefaultSpec implements Spec {

      BaseSpec baseSpec;

      DefaultSpec() {
        if ((DefaultBuilderConfig.getAdditionalConnectionFlags()
                & DestinationSetupFlyweight.FLAG_ALTERNATIVE_AUTHENTICATION)
            == DestinationSetupFlyweight.FLAG_ALTERNATIVE_AUTHENTICATION) {
          jwt();
        } else {
          simple();
        }
      }

      @Override
      public JwtSpec jwt() {
        DefaultJwtSpec jwtSpec = new DefaultJwtSpec();
        baseSpec = jwtSpec;
        return jwtSpec;
      }

      @Override
      public SimpleSpec simple() {
        DefaultSimpleSpec simpleSpec = new DefaultSimpleSpec();
        baseSpec = simpleSpec;
        return simpleSpec;
      }
    }

    abstract class BaseSpec {
      long accessKey;
      byte[] accessToken;

      abstract AuthenticationConfig build();
    }

    class DefaultSimpleSpec extends BaseSpec implements SimpleSpec {

      DefaultSimpleSpec() {
        Long key = DefaultBuilderConfig.getAccessKey();
        if (key != null) {
          key(key);
        }

        String token = DefaultBuilderConfig.getAccessToken();
        if (token != null) {
          token(token);
        }
      }

      @Override
      public SimpleSpec key(long accessKey) {
        this.accessKey = accessKey;
        return this;
      }

      @Override
      public SimpleSpec token(String bast64EncodedAccessToken) {
        return token(Base64.getDecoder().decode(bast64EncodedAccessToken));
      }

      @Override
      public SimpleSpec token(byte[] accessToken) {
        this.accessToken = accessToken;
        return this;
      }

      @Override
      AuthenticationConfig build() {
        return AuthenticationConfig.simple(accessKey, accessToken);
      }
    }

    class DefaultJwtSpec extends BaseSpec implements JwtSpec {

      DefaultJwtSpec() {
        token(DefaultBuilderConfig.getAccessToken());
      }

      @Override
      public JwtSpec token(String bast64EncodedAccessToken) {
        return token(Base64.getDecoder().decode(bast64EncodedAccessToken));
      }

      @Override
      public JwtSpec token(byte[] accessToken) {
        this.accessToken = accessToken;
        return this;
      }

      @Override
      AuthenticationConfig build() {
        return AuthenticationConfig.jwt(accessToken);
      }
    }

    final class JwtAuthenticationConfig implements AuthenticationConfig {

      private final byte[] jwtToken;

      JwtAuthenticationConfig(byte[] jwtToken) {
        this.jwtToken = jwtToken;
      }

      @Override
      public long accessKey() {
        return DestinationSetupFlyweight.JWT_AUTHENTICATION;
      }

      @Override
      public byte[] accessToken() {
        return jwtToken;
      }
    }
  }

  public interface DestinationInfoConfig {
    String group();

    Tags tags();

    InetAddress inetAddress();

    boolean isPublic();

    static Builder builder() {
      return new Builder();
    }

    class Builder {

      private String group = DefaultBuilderConfig.getGroup();
      private String destination = DefaultBuilderConfig.getDestination();
      private Tags tags = DefaultBuilderConfig.getTags();
      private InetAddress localINetAddress = DefaultBuilderConfig.getLocalAddress();
      private boolean isPublic =
          (DefaultBuilderConfig.getAdditionalConnectionFlags()
                  & DestinationSetupFlyweight.FLAG_ENABLE_PUBLIC_ACCESS)
              == DestinationSetupFlyweight.FLAG_ENABLE_PUBLIC_ACCESS;

      public Builder asPublicDestination() {
        isPublic = true;
        return this;
      }

      public Builder asPrivateDestination() {
        isPublic = false;
        return this;
      }

      public Builder groupName(String group) {
        this.group = group;
        return this;
      }

      public Builder localAddress(InetAddress localINetAddress) {
        this.localINetAddress = localINetAddress;
        return this;
      }

      public Builder destinationTag(String destination) {
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

      DestinationInfoConfig build() {
        Tags tags;
        if (destination == null || destination.isEmpty()) {
          destination = defaultDestination();
        }

        tags = this.tags.and("com.netifi.destination", destination);

        return new DestinationInfoConfig() {
          @Override
          public String group() {
            return group;
          }

          @Override
          public Tags tags() {
            return tags;
          }

          @Override
          public InetAddress inetAddress() {
            return localINetAddress;
          }

          @Override
          public boolean isPublic() {
            return isPublic;
          }
        };
      }
    }
  }

  public interface ConnectionConfig {

    Function<SocketAddress, ClientTransport> clientTransportFactory();

    Function<Broker, InetSocketAddress> brokerAddressSelector();

    String connectionName();

    static Builder builder() {
      return new Builder();
    }

    static WebSocketBuilder ws() {
      return new WebSocketBuilder();
    }

    static TcpBuilder tcp() {
      return new TcpBuilder();
    }

    interface Spec {
      TcpBuilder tcp();

      WebSocketBuilder ws();

      Builder custom();
    }

    class DefaultSpec implements Spec {

      BaseBuilder baseBuilder;

      @Override
      public TcpBuilder tcp() {
        TcpBuilder tcpBuilder = ConnectionConfig.tcp();
        if (DefaultBuilderConfig.isSslDisabled()) {
          tcpBuilder.ssl(SslConfig.Spec::unsecured);
        } else {
          tcpBuilder.ssl(SslConfig.Spec::secured);
        }
        baseBuilder = tcpBuilder;
        return tcpBuilder;
      }

      @Override
      public WebSocketBuilder ws() {
        WebSocketBuilder webSocketBuilder = ConnectionConfig.ws();
        if (DefaultBuilderConfig.isSslDisabled()) {
          webSocketBuilder.ssl(SslConfig.Spec::unsecured);
        } else {
          webSocketBuilder.ssl(SslConfig.Spec::secured);
        }
        baseBuilder = webSocketBuilder;
        return webSocketBuilder;
      }

      @Override
      public Builder custom() {
        Builder builder = ConnectionConfig.builder();
        baseBuilder = builder;
        return builder;
      }
    }

    abstract class BaseBuilder<SELF extends BaseBuilder<SELF>> {
      String connectionName;

      public BaseBuilder() {
        connectionName = DefaultBuilderConfig.getConnectionId();

        if (connectionName == null || connectionName.isEmpty()) {
          connectionName = UUID.randomUUID().toString();
        }
      }

      public SELF withConnectionName(String connectionName) {
        this.connectionName = connectionName;
        return (SELF) this;
      }

      abstract ConnectionConfig build();
    }

    abstract class TcpBasedBuilder<SELF extends TcpBasedBuilder<SELF>> extends BaseBuilder<SELF> {

      Consumer<SslConfig.Spec> specBuilder = __ -> {};

      public SELF ssl(Consumer<SslConfig.Spec> specBuilder) {
        this.specBuilder = Objects.requireNonNull(specBuilder);
        return (SELF) this;
      }

      static <T extends ClientTransport> ConnectionConfig tcpBased(
          Function<TcpClient, T> transportFactory,
          Function<Broker, InetSocketAddress> brokerAddressSelector,
          String connectionName,
          Consumer<SslConfig.Spec> sslBuilder) {
        SslConfig.DefaultSpec spec = new SslConfig.DefaultSpec();
        sslBuilder.accept(spec);
        SslConfig sslConfig = spec.sslConfig;

        if (sslConfig instanceof SslConfig.NoSslConfig) {
          return new ConnectionConfig() {
            @Override
            public Function<SocketAddress, ClientTransport> clientTransportFactory() {
              return address -> {
                TcpClient client = TcpClient.create().addressSupplier(() -> address);
                return transportFactory.apply(client);
              };
            }

            @Override
            public Function<Broker, InetSocketAddress> brokerAddressSelector() {
              return brokerAddressSelector;
            }

            @Override
            public String connectionName() {
              return connectionName;
            }
          };

        } else {
          final SslContext sslContext = sslConfig.sslContextProvider().get();
          return new ConnectionConfig() {
            @Override
            public Function<SocketAddress, ClientTransport> clientTransportFactory() {
              return address -> {
                TcpClient client =
                    TcpClient.create()
                        .addressSupplier(() -> address)
                        .secure(sslContextSpec -> sslContextSpec.sslContext(sslContext));
                return transportFactory.apply(client);
              };
            }

            @Override
            public Function<Broker, InetSocketAddress> brokerAddressSelector() {
              return BrokerAddressSelectors.TCP_ADDRESS;
            }

            @Override
            public String connectionName() {
              return connectionName;
            }
          };
        }
      }
    }

    class TcpBuilder extends TcpBasedBuilder<TcpBuilder> {

      ConnectionConfig build() {
        return tcpBased(
            TcpClientTransport::create,
            BrokerAddressSelectors.TCP_ADDRESS,
            connectionName,
            specBuilder);
      }
    }

    class WebSocketBuilder extends TcpBasedBuilder<TcpBuilder> {

      ConnectionConfig build() {
        return tcpBased(
            WebsocketClientTransport::create,
            BrokerAddressSelectors.WEBSOCKET_ADDRESS,
            connectionName,
            specBuilder);
      }
    }

    class Builder extends BaseBuilder<Builder> {
      Function<SocketAddress, ClientTransport> clientTransportFactory;
      Function<Broker, InetSocketAddress> brokerAddressSelector;

      public Builder withTransportFactory(
          Function<SocketAddress, ClientTransport> clientTransportFactory) {
        this.clientTransportFactory = clientTransportFactory;
        return this;
      }

      public Builder withBrokerAddressSelector(
          Function<Broker, InetSocketAddress> brokerAddressSelector) {
        this.brokerAddressSelector = brokerAddressSelector;
        return this;
      }

      @Override
      public ConnectionConfig build() {
        return new ConnectionConfig() {
          @Override
          public Function<SocketAddress, ClientTransport> clientTransportFactory() {
            return clientTransportFactory;
          }

          @Override
          public Function<Broker, InetSocketAddress> brokerAddressSelector() {
            return brokerAddressSelector;
          }

          @Override
          public String connectionName() {
            return connectionName;
          }
        };
      }
    }
  }

  public static final class ClientBuilder {

    private AuthenticationConfig.DefaultSpec authenticationSpec =
        new AuthenticationConfig.DefaultSpec();
    private DestinationInfoConfig.Builder destinationInfoConfigBuilder =
        DestinationInfoConfig.builder();
    private ConnectionConfig.DefaultSpec connectionSpec = new ConnectionConfig.DefaultSpec();
    private KeepAliveConfig.DefaultSpec keepAliveConfigSpec = new KeepAliveConfig.DefaultSpec();
    private DiscoveryConfig.DefaultSpec discoveryConfigSpec = new DiscoveryConfig.DefaultSpec();
    private int poolSize = DefaultBuilderConfig.getPoolSize();
    private Tracer tracer;

    private Consumer<AuthenticationConfig.Spec> authenticationBuilder = (__) -> {};
    private Consumer<DestinationInfoConfig.Builder> destinationInfoBuilder = (__) -> {};
    private Consumer<KeepAliveConfig.Spec> keepAliveBuilder = (__) -> {};
    private Consumer<ConnectionConfig.Spec> connectionBuilder = (__) -> {};
    private Consumer<DiscoveryConfig.Spec> discoveryBuilder = (__) -> {};

    public ClientBuilder authentication(Consumer<AuthenticationConfig.Spec> authenticationBuilder) {
      this.authenticationBuilder =
          this.authenticationBuilder.andThen(Objects.requireNonNull(authenticationBuilder));
      return this;
    }

    public ClientBuilder destinationInfo(
        Consumer<DestinationInfoConfig.Builder> destinationInfoBuilder) {
      this.destinationInfoBuilder =
          this.destinationInfoBuilder.andThen(Objects.requireNonNull(destinationInfoBuilder));
      return this;
    }

    public ClientBuilder keepAlive(Consumer<KeepAliveConfig.Spec> keepAliveBuilder) {
      this.keepAliveBuilder =
          this.keepAliveBuilder.andThen(Objects.requireNonNull(keepAliveBuilder));
      return this;
    }

    public ClientBuilder connection(Consumer<ConnectionConfig.Spec> connectionBuilder) {
      this.connectionBuilder =
          this.connectionBuilder.andThen(Objects.requireNonNull(connectionBuilder));
      return this;
    }

    public ClientBuilder discoveryStrategy(Consumer<DiscoveryConfig.Spec> discoveryBuilder) {
      this.discoveryBuilder =
          this.discoveryBuilder.andThen(Objects.requireNonNull(discoveryBuilder));
      return this;
    }

    public ClientBuilder poolSize(int poolSize) {
      this.poolSize = poolSize;
      return this;
    }

    public ClientBuilder tracer(Tracer tracer) {
      this.tracer = tracer;
      return this;
    }

    public BrokerService toService() {
      return toService(new AbstractRSocket() {}, false);
    }

    public RoutingBrokerService toRoutingService() {
      return toRoutingService(new SimpleRouter());
    }

    public RoutingBrokerService toRoutingService(MutableRouter router) {
      DefaultBrokerService brokerService =
          (DefaultBrokerService) toService(new RoutingServerRSocket(router), true);
      return new DefaultRoutingBrokerService(router, brokerService);
    }

    public BrokerService toService(RSocket rSocket, boolean responderRequiresUnwrapping) {
      destinationInfoBuilder.accept(destinationInfoConfigBuilder);
      DestinationInfoConfig destinationInfoConfig = destinationInfoConfigBuilder.build();

      connectionBuilder.accept(connectionSpec);
      ConnectionConfig connectionConfig = connectionSpec.baseBuilder.build();

      discoveryBuilder.accept(discoveryConfigSpec);
      DiscoveryConfig discoveryConfig = discoveryConfigSpec.config;

      keepAliveBuilder.accept(keepAliveConfigSpec);
      KeepAliveConfig keepAliveConfig = keepAliveConfigSpec.builder.build();

      authenticationBuilder.accept(authenticationSpec);
      AuthenticationConfig authenticationConfig = authenticationSpec.baseSpec.build();

      short additionalFlags = DefaultBuilderConfig.getAdditionalConnectionFlags();

      if (destinationInfoConfig.isPublic()) {
        additionalFlags |= DestinationSetupFlyweight.FLAG_ENABLE_PUBLIC_ACCESS;
      }

      if (authenticationConfig instanceof AuthenticationConfig.JwtAuthenticationConfig) {
        additionalFlags |= DestinationSetupFlyweight.FLAG_ALTERNATIVE_AUTHENTICATION;
      }

      return new DefaultBrokerService(
          rSocket,
          responderRequiresUnwrapping,
          destinationInfoConfig.inetAddress(),
          destinationInfoConfig.group(),
          destinationInfoConfig.tags(),
          connectionConfig.connectionName(),
          connectionConfig.brokerAddressSelector(),
          connectionConfig.clientTransportFactory(),
          discoveryConfig.discoveryStrategy(),
          keepAliveConfig instanceof KeepAliveConfig.NoKeepAliveConfig,
          keepAliveConfig.tickPeriod(),
          keepAliveConfig.acknowledgeTimeout(),
          keepAliveConfig.missedAcknowledges(),
          authenticationConfig.accessKey(),
          Unpooled.wrappedBuffer(authenticationConfig.accessToken()),
          additionalFlags,
          poolSize,
          tracer);
    }
  }
}
