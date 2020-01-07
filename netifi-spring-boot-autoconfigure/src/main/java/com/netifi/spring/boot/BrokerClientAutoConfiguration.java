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
import com.netifi.broker.BrokerFactory;
import com.netifi.broker.BrokerService;
import com.netifi.broker.RoutingBrokerService;
import com.netifi.broker.discovery.*;
import com.netifi.broker.micrometer.BrokerMeterRegistrySupplier;
import com.netifi.broker.tracing.BrokerTracerSupplier;
import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import com.netifi.spring.boot.support.BrokerServiceConfigurer;
import com.netifi.spring.core.BrokerClientTagSupplier;
import com.netifi.spring.core.config.BrokerClientConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.ipc.MutableRouter;
import io.rsocket.ipc.Router;
import io.rsocket.ipc.routing.SimpleRouter;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnNotWebApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.event.ApplicationFailedEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.core.annotation.Order;
import org.springframework.util.StringUtils;

@Configuration
@EnableConfigurationProperties(BrokerClientProperties.class)
@AutoConfigureBefore(BrokerClientConfiguration.class)
public class BrokerClientAutoConfiguration {

  static RoutingBrokerService configureBrokerClient(
      MutableRouter router, List<? extends BrokerServiceConfigurer> configurers) {
    BrokerFactory.ClientBuilder clientBuilder = BrokerFactory.connect();

    AnnotationAwareOrderComparator.sort(configurers);

    for (BrokerServiceConfigurer configurer : configurers) {
      configurer.configure(clientBuilder);
    }

    return clientBuilder.toRoutingService(router);
  }

  @Bean(name = "internalScanClassPathBeanDefinitionRegistryPostProcessor")
  public BeanDefinitionRegistryPostProcessor scanClassPathBeanDefinitionRegistryPostProcessor() {
    return new ScanClassPathBeanDefinitionRegistryPostProcessor();
  }

  @Bean
  @Order(Ordered.HIGHEST_PRECEDENCE)
  public BrokerServiceConfigurer propertiesBasedBrokerClientConfigurer(
      BrokerClientTagSupplier brokerClientTagSupplier,
      BrokerClientProperties brokerClientProperties) {
    return builder -> {
      BrokerClientProperties.SslProperties ssl = brokerClientProperties.getSsl();
      BrokerClientProperties.AccessProperties access = brokerClientProperties.getAccess();
      BrokerClientProperties.BrokerProperties broker = brokerClientProperties.getBroker();
      BrokerClientProperties.DiscoveryProperties discovery = brokerClientProperties.getDiscovery();
      BrokerClientProperties.KeepAliveProperties keepalive = brokerClientProperties.getKeepalive();
      BrokerClientProperties.ConnectionType connectionType;
      DiscoveryStrategy discoveryStrategy;

      if (!StringUtils.isEmpty(broker.getHostname())) {
        // support the legacy usecase first
        discoveryStrategy =
            new StaticListDiscoveryStrategy(
                new StaticListDiscoveryConfig(broker.getPort(), broker.getHostname()));

        connectionType = broker.getConnectionType();

      } else if (!StringUtils.isEmpty(discovery.getEnvironment())) {
        // if not legacy, then we're propbably using the new discovery api.
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

      } else {
        throw new RuntimeException("discovery not configured and required");
      }
      builder
          .discoveryStrategy(spec -> spec.custom(discoveryStrategy))
          .connection(
              spec -> {
                boolean sslDisabled = ssl.isDisabled();
                BrokerFactory.ConnectionConfig.TcpBasedBuilder<?> tcpSpec = null;
                switch (connectionType) {
                  case TCP:
                    tcpSpec = spec.tcp();
                    break;
                  case WS:
                    tcpSpec = spec.ws();
                    break;
                }

                if (tcpSpec != null) {
                  tcpSpec.ssl(
                      sslSpec -> {
                        if (sslDisabled) {
                          sslSpec.unsecured();
                        } else {
                          sslSpec.secured();
                        }
                      });
                }
              })
          .destinationInfo(
              spec -> {
                if (!StringUtils.isEmpty(brokerClientProperties.getDestination())) {
                  spec.destinationTag(brokerClientProperties.getDestination());
                }

                Tags tags = Tags.empty();
                if (brokerClientProperties.getTags() != null
                    && !brokerClientProperties.getTags().isEmpty()) {
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
                if (brokerClientProperties.isPublic()) {
                  spec.asPublicDestination();
                } else {
                  spec.asPrivateDestination();
                }

                spec.tags(tags).groupName(brokerClientProperties.getGroup());
              })
          .keepAlive(
              spec -> {
                if (keepalive.isEnabled()) {
                  spec.configure()
                      .acknowledgeTimeout(Duration.ofSeconds(keepalive.getAckTimeoutSeconds()))
                      .tickPeriod(Duration.ofSeconds(keepalive.getTickPeriodSeconds()))
                      .missedAcknowledges(keepalive.getMissedAcks());
                }
              })
          .authentication(spec -> spec.simple().token(access.getToken()).key(access.getKey()))
          .poolSize(brokerClientProperties.getPoolSize());
    };
  }

  @Configuration
  @ConditionalOnMissingBean(BrokerClientTagSupplier.class)
  public static class BrokerTagSupplierConfigurations {
    @Bean
    public BrokerClientTagSupplier brokerClientTagSupplier() {
      return Tags::empty;
    }
  }

  @Configuration
  @ConditionalOnMissingBean(Router.class)
  public static class RouterConfiguration {
    @Bean
    public MutableRouter mutableRouter() {
      return new SimpleRouter();
    }
  }

  @Configuration
  @ConditionalOnMissingBean(MeterRegistry.class)
  @ConditionalOnClass(BrokerMeterRegistrySupplier.class)
  public static class MetricsConfigurations {

    @Bean
    public MeterRegistry meterRegistry(
        BrokerService brokerClient, BrokerClientProperties properties) {
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
    public Tracer tracer(BrokerService brokerClient, BrokerClientProperties properties) {
      return new BrokerTracerSupplier(brokerClient, Optional.of(properties.getTracing().getGroup()))
          .get();
    }
  }

  @Configuration
  @ConditionalOnNotWebApplication
  @ConditionalOnMissingBean({BrokerService.class})
  public static class NonWebBrokerClientConfiguration {

    @Bean
    public RoutingBrokerService routingBrokerService(
        MutableRouter router,
        List<? extends BrokerServiceConfigurer> configurers,
        ConfigurableApplicationContext context) {
      RoutingBrokerService brokerClient = configureBrokerClient(router, configurers);

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

    private void startDaemonAwaitThread(BrokerService brokerClient) {
      Thread awaitThread =
          new Thread("broker-service-thread") {

            @Override
            public void run() {
              brokerClient.onClose().block();
            }
          };
      awaitThread.setContextClassLoader(getClass().getClassLoader());
      awaitThread.setDaemon(false);
      awaitThread.start();
    }

    @Bean
    public BrokerClient routingBrokerService(RoutingBrokerService routingBrokerService) {
      return BrokerClient.from(routingBrokerService);
    }
  }

  @Configuration
  @ConditionalOnWebApplication
  @ConditionalOnMissingBean({BrokerService.class})
  public static class WebBrokerClientConfiguration {

    @Bean
    public RoutingBrokerService routingBrokerService(
        MutableRouter router, List<? extends BrokerServiceConfigurer> configurers) {
      return configureBrokerClient(router, configurers);
    }

    @Bean
    public BrokerClient routingBrokerService(RoutingBrokerService routingBrokerService) {
      return BrokerClient.from(routingBrokerService);
    }
  }
}
