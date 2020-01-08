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
package com.netifi.spring.core.annotation;

import com.netifi.broker.rsocket.BrokerSocket;
import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BroadcastAwareClientFactory;
import com.netifi.spring.core.BrokerClientFactory;
import com.netifi.spring.core.BrokerClientFactorySupport;
import com.netifi.spring.core.DestinationAwareClientFactory;
import com.netifi.spring.core.GroupAwareClientFactory;
import com.netifi.spring.core.NoTagsSupplier;
import com.netifi.spring.core.TagSupplier;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.RSocket;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.AnnotatedGenericBeanDefinition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Processes custom dependency injection for fields marked with the {@link BrokerClient} annotation.
 */
public class BrokerClientStaticFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerClientStaticFactory.class);

  /**
   * Creates an instance of the correct Netifi Broker client for injection into a annotated field.
   *
   * @return an instance of a {@link com.netifi.broker.BrokerService} client
   */
  public static Object getBeanInstance(
      final DefaultListableBeanFactory beanFactory,
      final ResolvableType targetClass,
      final BrokerClient brokerClientAnnotation,
      final Collection<BrokerClientFactorySupport> brokerClientFactories) {
    final Class<?> resolvedTargetClass = targetClass.toClass();
    final String beanName = getBeanName(brokerClientAnnotation, resolvedTargetClass);

    if (!beanFactory.containsBean(beanName)) {
      final Tags suppliedTags = resolveTags(beanFactory, brokerClientAnnotation);

      Object toRegister = null;
      try {
        //                String[] tracerSupplierBeanNames =
        //                    beanFactory.getBeanNamesForType(ResolvableType.forClassWithGenerics(
        //                        Supplier.class,
        //                        Tracer.class));
        //                String[] meterRegistrySupplierBeanNames =
        //                    beanFactory.getBeanNamesForType(ResolvableType.forClassWithGenerics(
        //                        Supplier.class,
        //                        MeterRegistry.class));
        //
        //                Tracer tracer = null;
        //                MeterRegistry meterRegistry = null;
        //
        //                // Tracers
        //                if (tracerSupplierBeanNames.length >= 1) {
        //                    if (tracerSupplierBeanNames.length > 1) {
        //                        LOGGER.warn(
        //                            "More than one implementation of Tracer detected on the
        // classpath. Arbitrarily choosing one to use.");
        //                    }
        //
        //                    Supplier<Tracer> tracerSupplier =
        //                        (Supplier<Tracer>)
        // beanFactory.getBean(tracerSupplierBeanNames[0]);
        //                    tracer = tracerSupplier.get();
        //                }
        //
        //                // Meter Registries
        //                if (meterRegistrySupplierBeanNames.length >= 1) {
        //                    if (meterRegistrySupplierBeanNames.length > 1) {
        //                        LOGGER.warn(
        //                            "More than one implementation of MeterRegistry detected on the
        // classpath. Arbitrarily choosing one to use.");
        //                    }
        //
        //                    Supplier<MeterRegistry> meterRegistrySupplier =
        //                        (Supplier<MeterRegistry>) beanFactory.getBean(
        //                            meterRegistrySupplierBeanNames[0]);
        //                    meterRegistry = meterRegistrySupplier.get();
        //                }
        //                else {
        //                    // Fallback to MeterRegistry implementations on the classpath if we
        // can't find any suppliers
        //                    Map<String, MeterRegistry> meterRegistryBeans =
        //                        beanFactory.getBeansOfType(MeterRegistry.class);
        //
        //                    if (!meterRegistryBeans.isEmpty()) {
        //                        if (meterRegistryBeans.size() > 1) {
        //                            LOGGER.warn(
        //                                "More than one implementation of MeterRegistry detected on
        // the classpath. Arbitrarily choosing one to use.");
        //                        }
        //
        //                        meterRegistry = (MeterRegistry) meterRegistryBeans.values()
        //                                                                          .toArray()[0];
        //                    }
        //                }

        if (BrokerClientFactory.class.isAssignableFrom(resolvedTargetClass)) {
          Class<?> clientClass = brokerClientAnnotation.clientClass();

          if (clientClass.equals(Void.class)) {
            ResolvableType clientResolvableType = targetClass.getGeneric(0);

            if (clientResolvableType == ResolvableType.NONE) {
              throw new RuntimeException(
                  "Instantiating BrokerClientFactory requires target client class");
            }

            clientClass = clientResolvableType.toClass();
          }

          for (BrokerClientFactorySupport brokerClientFactory : brokerClientFactories) {
            if (brokerClientFactory.support(clientClass)) {
              toRegister =
                  createBrokerClientFactory(
                      clientClass,
                      resolvedTargetClass,
                      brokerClientFactory,
                      brokerClientAnnotation.type(),
                      brokerClientAnnotation.group(),
                      brokerClientAnnotation.destination(),
                      suppliedTags);
            }
          }
        } else {
          for (BrokerClientFactorySupport brokerClientFactory : brokerClientFactories) {
            if (brokerClientFactory.support(resolvedTargetClass)) {
              toRegister =
                  brokerClientFactory.lookup(
                      resolvedTargetClass,
                      brokerClientAnnotation.type(),
                      brokerClientAnnotation.group(),
                      suppliedTags);
              break;
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(String.format("Error injecting bean '%s'", targetClass), e);
      }

      if (toRegister == null) {
        throw new RuntimeException(String.format("Unsupported bean '%s'", targetClass));
      }

      Object newInstance = beanFactory.initializeBean(toRegister, beanName);
      beanFactory.autowireBeanProperties(
          newInstance, AutowireCapableBeanFactory.AUTOWIRE_BY_NAME, true);
      AnnotatedGenericBeanDefinition beanDefinition =
          new AnnotatedGenericBeanDefinition(resolvedTargetClass);
      AutowireCandidateQualifier qualifier = new AutowireCandidateQualifier(Qualifier.class);

      qualifier.setAttribute("value", "client");
      beanDefinition.addQualifier(qualifier);

      beanFactory.registerBeanDefinition(beanName, beanDefinition);
      beanFactory.registerSingleton(beanName, newInstance);

      LOGGER.debug("Bean named '{}' created successfully.", beanName);

      return newInstance;
    } else {
      LOGGER.debug("Bean named '{}' already exists, using as current bean reference.", beanName);
      return beanFactory.getBean(beanName);
    }
  }

  public static Tags resolveTags(
      DefaultListableBeanFactory beanFactory, BrokerClient brokerClientAnnotation) {
    // Tags reconciliation
    TagSupplier tagSupplier = NoTagsSupplier.INSTANCE;
    brokerClientAnnotation.tagSupplier();
    if (!brokerClientAnnotation.tagSupplier().equals(NoTagsSupplier.class)) {
      tagSupplier = beanFactory.getBean(brokerClientAnnotation.tagSupplier());
    }

    Tags suppliedTags = tagSupplier.get();
    for (Tag t : brokerClientAnnotation.tags()) {
      if (!suppliedTags.stream().anyMatch(tag -> tag.getKey().equals(t.name()))) {
        suppliedTags = suppliedTags.and(com.netifi.common.tags.Tag.of(t.name(), t.value()));
      }
    }
    return suppliedTags;
  }

  /**
   * Generates a unique bean name for the field.
   *
   * @param brokerClientAnnotation annotation data
   * @param clazz target class
   * @return bean name
   */
  private static String getBeanName(BrokerClient brokerClientAnnotation, Class<?> clazz) {
    Assert.hasText(brokerClientAnnotation.group(), "@BrokerClient.group() must be specified");

    String beanName =
        clazz.getSimpleName()
            + "_"
            + brokerClientAnnotation.type().toString().toLowerCase()
            + "_"
            + brokerClientAnnotation.group();

    if (!StringUtils.isEmpty(brokerClientAnnotation.destination())) {
      beanName += "_" + brokerClientAnnotation.destination();
    }

    return beanName;
  }

  static <T> T createBrokerClient(
      com.netifi.broker.BrokerService brokerClient,
      BrokerClient.Type routeType,
      String group,
      String destination,
      Tags tags,
      Tracer tracer,
      MeterRegistry meterRegistry,
      Class<T> clientClass)
      throws NoSuchMethodException, InstantiationException, IllegalAccessException,
          InvocationTargetException {
    // Creating default BrokerSocket Instance
    BrokerSocket brokerSocket =
        createBrokerRSocket(brokerClient, routeType, group, destination, tags);

    T toRegister;

    if (tracer == null && meterRegistry == null) {
      // No Tracer or MeterRegistry
      Constructor ctor = clientClass.getConstructor(RSocket.class);
      toRegister = (T) ctor.newInstance(brokerSocket);
    } else if (tracer != null && meterRegistry == null) {
      // Tracer Only
      Constructor ctor = clientClass.getConstructor(RSocket.class, Tracer.class);
      toRegister = (T) ctor.newInstance(brokerSocket, tracer);
    } else if (tracer == null && meterRegistry != null) {
      // MeterRegistry Only
      Constructor ctor = clientClass.getConstructor(RSocket.class, MeterRegistry.class);
      toRegister = (T) ctor.newInstance(brokerSocket, meterRegistry);
    } else {
      // Both Tracer and MeterRegistry
      Constructor ctor =
          clientClass.getConstructor(RSocket.class, MeterRegistry.class, Tracer.class);
      toRegister = (T) ctor.newInstance(brokerSocket, meterRegistry, tracer);
    }
    return toRegister;
  }

  public static BrokerSocket createBrokerRSocket(
      com.netifi.broker.BrokerService brokerClient,
      BrokerClient.Type routeType,
      String group,
      String destination,
      Tags tags) {
    BrokerSocket brokerSocket = null;

    switch (routeType) {
      case BROADCAST:
        brokerSocket = brokerClient.broadcast(group, tags);
        break;
      case GROUP:
        brokerSocket = brokerClient.group(group, tags);
        break;
      case DESTINATION:
        brokerSocket =
            brokerClient.group(
                group,
                StringUtils.isEmpty(destination)
                    ? tags
                    : Tags.of("com.netifi.destination", destination).and(tags));
        break;
    }
    return brokerSocket;
  }

  public static <T> BrokerClientFactory<T> createBrokerClientFactory(
      Class<T> clientClass,
      Class<?> targetClass,
      BrokerClientFactorySupport brokerClientFactorySupport,
      BrokerClient.Type routeType,
      String group,
      String destination,
      Tags tags) {

    final BaseBrokerClientFactory<T> brokerClientFactory =
        new BaseBrokerClientFactory<>(
            brokerClientFactorySupport, routeType, destination, clientClass, group, tags);

    if (targetClass.isAssignableFrom(GroupAwareClientFactory.class)) {
      return new DefaultGroupAwareClientFactory<>(brokerClientFactory);
    } else if (targetClass.isAssignableFrom(DestinationAwareClientFactory.class)) {
      return new DefaultDestinationAwareClientFactory<>(brokerClientFactory);
    } else if (targetClass.isAssignableFrom(BroadcastAwareClientFactory.class)) {
      return new DefaultBroadcastAwareClientFactory<>(brokerClientFactory);
    }

    return brokerClientFactory;
  }
}
