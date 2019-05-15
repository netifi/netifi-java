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
package com.netifi.spring.boot;

import com.netifi.broker.BrokerClient;
import com.netifi.broker.discovery.*;
import com.netifi.broker.micrometer.BrokerMeterRegistrySupplier;
import com.netifi.broker.rsocket.transport.BrokerAddressSelectors;
import com.netifi.broker.tracing.BrokerTracerSupplier;
import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import com.netifi.spring.boot.support.BrokerClientConfigurer;
import com.netifi.spring.core.BrokerClientTagSupplier;
import com.netifi.spring.core.config.BrokerClientConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentracing.Tracer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnNotWebApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.core.annotation.Order;
import org.springframework.util.StringUtils;
import reactor.core.Exceptions;
import reactor.netty.tcp.TcpClient;

@Configuration
@EnableConfigurationProperties(BrokerClientProperties.class)
@AutoConfigureBefore(BrokerClientConfiguration.class)
public class BrokerClientAutoConfiguration {

  static BrokerClient configureBrokerClient(List<? extends BrokerClientConfigurer> configurers) {
    BrokerClient.CustomizableBuilder builder = BrokerClient.customizable();

    AnnotationAwareOrderComparator.sort(configurers);

    for (BrokerClientConfigurer configurer : configurers) {
      builder = configurer.configure(builder);
    }

    return builder.build();
  }

  @Bean(name = "internalScanClassPathBeanDefinitionRegistryPostProcessor")
  public BeanDefinitionRegistryPostProcessor scanClassPathBeanDefinitionRegistryPostProcessor(
      ApplicationContext applicationContext) throws BeansException {
    return new ScanClassPathBeanDefinitionRegistryPostProcessor();
  }

  @Bean
  @Order(Ordered.HIGHEST_PRECEDENCE)
  public BrokerClientConfigurer propertiesBasedBrokerClientConfigurer(
      BrokerClientTagSupplier brokerClientTagSupplier,
      BrokerClientProperties brokerClientProperties) {
    return builder -> {
      BrokerClientProperties.SslProperties ssl = brokerClientProperties.getSsl();
      BrokerClientProperties.AccessProperties access = brokerClientProperties.getAccess();
      BrokerClientProperties.BrokerProperties broker = brokerClientProperties.getBroker();
      BrokerClientProperties.DiscoveryProperties discovery = brokerClientProperties.getDiscovery();

      if (!StringUtils.isEmpty(brokerClientProperties.getDestination())) {
        builder.destination(brokerClientProperties.getDestination());
      }

      BrokerClientProperties.ConnectionType connectionType;

      if (!StringUtils.isEmpty(broker.getHostname())) {
        // support the legacy usecase first
        builder.host(broker.getHostname());
        builder.port(broker.getPort());

        connectionType = broker.getConnectionType();

      } else if (!StringUtils.isEmpty(discovery.getEnvironment())) {
        // if not legacy, then we're propbably using the new discovery api.
        DiscoveryStrategy discoveryStrategy;
        switch (discovery.getEnvironment()) {
          case "static":
            BrokerClientProperties.DiscoveryProperties.StaticProperties staticProperties =
                discovery.getStaticProperties();
            StaticListDiscoveryConfig staticListDiscoveryConfig =
                new StaticListDiscoveryConfig(
                    staticProperties.getPort(), staticProperties.getAddresses());
            discoveryStrategy = DiscoveryStrategy.getInstance(staticListDiscoveryConfig);
            connectionType = staticProperties.getConnectionType();
            break;
          case "ec2":
            BrokerClientProperties.DiscoveryProperties.EC2Properties ec2Properties =
                discovery.getEc2Properties();
            EC2TagsDiscoveryConfig ec2TagsDiscoveryConfig =
                new EC2TagsDiscoveryConfig(
                    ec2Properties.getTagName(),
                    ec2Properties.getTagValue(),
                    ec2Properties.getPort());
            discoveryStrategy = DiscoveryStrategy.getInstance(ec2TagsDiscoveryConfig);
            connectionType = ec2Properties.getConnectionType();
            break;
          case "consul":
            BrokerClientProperties.DiscoveryProperties.ConsulProperties consulProperties =
                discovery.getConsulProperties();
            ConsulDiscoveryConfig consulDiscoveryConfig =
                new ConsulDiscoveryConfig(
                    consulProperties.getConsulURL(), consulProperties.getServiceName());
            discoveryStrategy = DiscoveryStrategy.getInstance(consulDiscoveryConfig);
            connectionType = consulProperties.getConnectionType();
            break;
          case "kubernetes":
            BrokerClientProperties.DiscoveryProperties.KubernetesProperties kubernetesProperties =
                discovery.getKubernetesProperties();
            KubernetesDiscoveryConfig kubernetesDiscoveryConfig =
                new KubernetesDiscoveryConfig(
                    kubernetesProperties.getNamespace(),
                    kubernetesProperties.getDeploymentName(),
                    kubernetesProperties.getPortName());
            discoveryStrategy = DiscoveryStrategy.getInstance(kubernetesDiscoveryConfig);
            connectionType = kubernetesProperties.getConnectionType();
            break;
          default:
            throw new RuntimeException(
                "unsupported discovery strategy " + discovery.getEnvironment());
        }
        builder.discoveryStrategy(discoveryStrategy);
      } else {
        throw new RuntimeException("discovery not configured and required");
      }

      Tags tags = Tags.empty();
      if (brokerClientProperties.getTags() != null && !brokerClientProperties.getTags().isEmpty()) {
        for (String t : brokerClientProperties.getTags()) {
          String[] split = t.split(":");
          Tag tag = Tag.of(split[0], split[1]);
          tags = tags.and(tag);
        }
      }

      Tags suppliedTags = brokerClientTagSupplier.get();

      if (suppliedTags != null) {
        tags = tags.and(suppliedTags);
      }

      boolean sslDisabled = brokerClientProperties.getSsl().isDisabled();

      if (connectionType == BrokerClientProperties.ConnectionType.TCP) {
        builder.addressSelector(BrokerAddressSelectors.TCP_ADDRESS);
        builder.clientTransportFactory(
            address -> {
              if (sslDisabled) {
                TcpClient client = TcpClient.create().addressSupplier(() -> address);
                return TcpClientTransport.create(client);
              } else {
                TcpClient client =
                    TcpClient.create()
                        .addressSupplier(() -> address)
                        .secure(
                            spec -> {
                              final SslProvider sslProvider;
                              if (OpenSsl.isAvailable()) {
                                sslProvider = SslProvider.OPENSSL_REFCNT;
                              } else {
                                sslProvider = SslProvider.JDK;
                              }

                              try {
                                spec.sslContext(
                                    SslContextBuilder.forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .sslProvider(sslProvider)
                                        .build());
                              } catch (Exception sslException) {
                                throw Exceptions.propagate(sslException);
                              }
                            });

                return TcpClientTransport.create(client);
              }
            });
      } else if (connectionType == BrokerClientProperties.ConnectionType.WS) {
        builder.addressSelector(BrokerAddressSelectors.WEBSOCKET_ADDRESS);
        builder.tags(tags);
        builder.clientTransportFactory(
            address -> {
              if (sslDisabled) {
                TcpClient client = TcpClient.create().addressSupplier(() -> address);
                return WebsocketClientTransport.create(client);
              } else {
                TcpClient client =
                    TcpClient.create()
                        .addressSupplier(() -> address)
                        .secure(
                            spec -> {
                              final SslProvider sslProvider;
                              if (OpenSsl.isAvailable()) {
                                sslProvider = SslProvider.OPENSSL_REFCNT;
                              } else {
                                sslProvider = SslProvider.JDK;
                              }

                              try {
                                spec.sslContext(
                                    SslContextBuilder.forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .sslProvider(sslProvider)
                                        .build());
                              } catch (Exception sslException) {
                                throw Exceptions.propagate(sslException);
                              }
                            });
                return WebsocketClientTransport.create(client);
              }
            });
      }

      return builder
          .accessKey(access.getKey())
          .accessToken(access.getToken())
          .group(brokerClientProperties.getGroup())
          .poolSize(brokerClientProperties.getPoolSize());
    };
  }

  @Configuration
  @ConditionalOnMissingBean(BrokerClientTagSupplier.class)
  public static class BrokerTagSupplierConfiguations {
    @Bean
    public BrokerClientTagSupplier brokerClientTagSupplier() {
      return Tags::empty;
    }
  }

  @Configuration
  @ConditionalOnMissingBean(MeterRegistry.class)
  @ConditionalOnClass(BrokerMeterRegistrySupplier.class)
  public static class MetricsConfigurations {

    @Bean
    public MeterRegistry meterRegistry(
        BrokerClient brokerClient, BrokerClientProperties properties) {
      return new BrokerMeterRegistrySupplier(
              brokerClient,
              Optional.of(properties.getMetrics().getGroup()),
              Optional.of(properties.getMetrics().getReportingStepInMillis()),
              Optional.of(properties.getMetrics().isExport()))
          .get();
    }
  }

  @Configuration
  @ConditionalOnMissingBean(Tracer.class)
  @ConditionalOnClass(BrokerTracerSupplier.class)
  public static class TracingConfigurations {

    @Bean
    public Tracer tracer(BrokerClient brokerClient, BrokerClientProperties properties) {
      return new BrokerTracerSupplier(brokerClient, Optional.of(properties.getTracing().getGroup()))
          .get();
    }
  }

  @Configuration
  @ConditionalOnNotWebApplication
  @ConditionalOnMissingBean(BrokerClient.class)
  public static class NonWebBrokerClientConfiguration {

    @Bean
    public BrokerClient brokerClient(
        List<? extends BrokerClientConfigurer> configurers,
        ConfigurableApplicationContext context) {
      BrokerClient brokerClient = configureBrokerClient(configurers);

      startDaemonAwaitThread(brokerClient);

      context.addApplicationListener(
          event -> {
            if (event instanceof ContextClosedEvent
                || event instanceof ContextStoppedEvent
                || event instanceof ApplicationFailedEvent) {
              brokerClient.dispose();
            }
          });

      return brokerClient;
    }

    private void startDaemonAwaitThread(BrokerClient brokerClient) {
      Thread awaitThread =
          new Thread("broker-client-thread") {

            @Override
            public void run() {
              brokerClient.onClose().block();
            }
          };
      awaitThread.setContextClassLoader(getClass().getClassLoader());
      awaitThread.setDaemon(false);
      awaitThread.start();
    }
  }

  @Configuration
  @ConditionalOnWebApplication
  @ConditionalOnMissingBean(BrokerClient.class)
  public static class WebBrokerClientConfiguration {

    @Bean
    public BrokerClient brokerClient(List<? extends BrokerClientConfigurer> configurers) {
      return configureBrokerClient(configurers);
    }
  }
}
