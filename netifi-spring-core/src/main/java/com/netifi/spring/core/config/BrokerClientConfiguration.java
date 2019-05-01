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
package com.netifi.spring.core.config;

import com.netifi.broker.BrokerClient;
import com.netifi.broker.info.BlockingBrokerInfoService;
import com.netifi.broker.info.BlockingBrokerInfoServiceClient;
import com.netifi.broker.info.BlockingBrokerInfoServiceServer;
import com.netifi.broker.info.BrokerInfoService;
import com.netifi.broker.info.BrokerInfoServiceClient;
import com.netifi.broker.info.BrokerInfoServiceServer;
import com.netifi.spring.core.BrokerClientApplicationEventListener;
import com.netifi.spring.core.annotation.BrokerClientBeanDefinitionRegistryPostProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.rpc.metrics.om.BlockingMetricsSnapshotHandler;
import io.rsocket.rpc.metrics.om.BlockingMetricsSnapshotHandlerClient;
import io.rsocket.rpc.metrics.om.BlockingMetricsSnapshotHandlerServer;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandler;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandlerClient;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandlerServer;
import java.util.Optional;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.AnnotatedBeanDefinitionReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;

@Configuration
public class BrokerClientConfiguration implements ApplicationContextAware {

  @Bean(name = "internalBrokerClientBeanDefinitionRegistryPostProcessor")
  public BrokerClientBeanDefinitionRegistryPostProcessor
      brokerClientBeanDefinitionRegistryPostProcessor() {
    return new BrokerClientBeanDefinitionRegistryPostProcessor();
  }

  @Bean
  public BrokerClientApplicationEventListener brokerClientApplicationEventListener(
      BrokerClient brokerClient) {
    return new BrokerClientApplicationEventListener(brokerClient);
  }

  @Bean
  public BrokerInfoServiceServer brokerInfoServiceServer(
      BrokerInfoService brokerInfoService,
      Optional<MeterRegistry> registry,
      Optional<Tracer> tracer) {
    return new BrokerInfoServiceServer(brokerInfoService, registry, tracer);
  }

  @Bean
  public BlockingBrokerInfoServiceServer blockingBrokerInfoServiceServer(
      BlockingBrokerInfoService blockingBrokerInfoService,
      Optional<Scheduler> scheduler,
      Optional<MeterRegistry> registry) {
    return new BlockingBrokerInfoServiceServer(blockingBrokerInfoService, scheduler, registry);
  }

  @Bean
  public MetricsSnapshotHandlerServer metricsSnapshotHandlerServer(
      MetricsSnapshotHandler metricsSnapshotHandler,
      Optional<MeterRegistry> registry,
      Optional<Tracer> tracer) {
    return new MetricsSnapshotHandlerServer(metricsSnapshotHandler, registry, tracer);
  }

  @Bean
  public BlockingMetricsSnapshotHandlerServer blockingMetricsSnapshotHandlerServer(
      BlockingMetricsSnapshotHandler blockingMetricsSnapshotHandler,
      Optional<Scheduler> scheduler,
      Optional<MeterRegistry> registry) {
    return new BlockingMetricsSnapshotHandlerServer(
        blockingMetricsSnapshotHandler, scheduler, registry);
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    DefaultListableBeanFactory factory =
        (DefaultListableBeanFactory) applicationContext.getAutowireCapableBeanFactory();
    AnnotatedBeanDefinitionReader reader = new AnnotatedBeanDefinitionReader(factory);

    reader.register(
        BlockingMetricsSnapshotHandlerClient.class,
        MetricsSnapshotHandlerClient.class,
        BlockingBrokerInfoServiceClient.class,
        BrokerInfoServiceClient.class);
  }
}
