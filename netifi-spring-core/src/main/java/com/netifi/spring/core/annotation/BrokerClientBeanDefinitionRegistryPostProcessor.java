/*
 *    Copyright 2020 The Netifi Authors
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

import com.netifi.spring.core.BrokerClientFactory;
import com.netifi.spring.core.BrokerClientFactorySupport;
import io.rsocket.rpc.RSocketRpcService;
import io.rsocket.rpc.annotations.internal.Generated;
import io.rsocket.rpc.annotations.internal.ResourceType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.DependencyDescriptor;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.AutowireCandidateResolver;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ContextAnnotationAutowireCandidateResolver;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.util.ClassUtils;

/** Handles post processing of custom Netifi Broker Client bean definitions. */
public class BrokerClientBeanDefinitionRegistryPostProcessor
    implements BeanDefinitionRegistryPostProcessor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BrokerClientBeanDefinitionRegistryPostProcessor.class);

  @Override
  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry)
      throws BeansException {
    DefaultListableBeanFactory beanFactory = unwrapDefaultListableBeanFactory(registry);

    beanFactory.setAutowireCandidateResolver(
        new ContextAnnotationAutowireCandidateResolver() {
          final AutowireCandidateResolver delegate = beanFactory.getAutowireCandidateResolver();

          @Override
          public Object getSuggestedValue(DependencyDescriptor descriptor) {
            BrokerClient annotation =
                descriptor.getField() == null
                    ? AnnotatedElementUtils.getMergedAnnotation(
                        descriptor.getMethodParameter().getParameter(), BrokerClient.class)
                    : AnnotatedElementUtils.getMergedAnnotation(
                        descriptor.getAnnotatedElement(), BrokerClient.class);

            if (annotation != null) {
              List<BrokerClientFactorySupport> brokerClientFactories =
                  new ArrayList<>(
                      beanFactory.getBeansOfType(BrokerClientFactorySupport.class).values());

              AnnotationAwareOrderComparator.sort(brokerClientFactories);

              Class<?> descriptorDeclaredType = descriptor.getDeclaredType();
              String[] beanNamesForType = beanFactory.getBeanNamesForType(descriptorDeclaredType);

              for (String beanName : beanNamesForType) {
                BeanDefinition beanDefinition = beanFactory.getMergedBeanDefinition(beanName);

                if (isAutowireCandidate(
                    new BeanDefinitionHolder(beanDefinition, beanName), descriptor)) {
                  return BrokerClientStaticFactory.getBeanInstance(
                      beanFactory,
                      resolveResolvableType(beanDefinition),
                      annotation,
                      brokerClientFactories);
                }
              }

              Generated generated = descriptorDeclaredType.getAnnotation(Generated.class);

              if ((generated != null && generated.type() == ResourceType.CLIENT)
                  || BrokerClientFactory.class.isAssignableFrom(descriptorDeclaredType)
                  || isSuportedByFactories(brokerClientFactories, descriptorDeclaredType)) {
                return BrokerClientStaticFactory.getBeanInstance(
                    beanFactory, descriptor.getResolvableType(), annotation, brokerClientFactories);
              }
            }

            return delegate.getSuggestedValue(descriptor);
          }
        });

    processAllGeneratedBeanDefinitions(beanFactory);
    processRSocketServiceBeanDefinitions(beanFactory);
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
      throws BeansException {}

  private static boolean isSuportedByFactories(
      Collection<BrokerClientFactorySupport> brokerClientFactories, Class<?> clazz) {
    for (BrokerClientFactorySupport factory : brokerClientFactories) {
      if (factory.support(clazz)) {
        return true;
      }
    }
    return false;
  }

  private static void processAllGeneratedBeanDefinitions(DefaultListableBeanFactory beanFactory) {
    for (String beanName : beanFactory.getBeanNamesForAnnotation(Generated.class)) {
      try {
        BeanDefinition definition = beanFactory.getBeanDefinition(beanName);
        Class<?> beanClass = resolveClass(definition);
        Generated generatedAnnotation = beanClass.getAnnotation(Generated.class);

        definition.setLazyInit(true);

        if (generatedAnnotation.type() == ResourceType.CLIENT) {
          if (definition instanceof AbstractBeanDefinition) {
            AbstractBeanDefinition beanDefinition = (AbstractBeanDefinition) definition;
            AutowireCandidateQualifier qualifier = new AutowireCandidateQualifier(Qualifier.class);

            qualifier.setAttribute("value", "client");
            beanDefinition.addQualifier(qualifier);
          }
        }
      } catch (ClassNotFoundException e) {
        LOGGER.error("Error during post processing of @Generated Netifi beans", e);
      }
    }
  }

  private static void processRSocketServiceBeanDefinitions(DefaultListableBeanFactory beanFactory) {
    for (String serviceServerBeanName : beanFactory.getBeanNamesForType(RSocketRpcService.class)) {
      try {
        BeanDefinition beanDefinition = beanFactory.getBeanDefinition(serviceServerBeanName);
        Class<?> clazz = resolveClass(beanDefinition);

        if (clazz != null && clazz.isAnnotationPresent(Generated.class)) {
          Generated brokerClientGeneratedAnnotation = clazz.getAnnotation(Generated.class);
          Class<?> idlClazz = brokerClientGeneratedAnnotation.idlClass();

          // Remove any AbstractRSocketRpcService beans that do not have an
          // implementation of their
          // underlying service in the bean registry.
          if (!findRealImplementationAndMarkAsPrimary(beanFactory, idlClazz)) {
            LOGGER.info(
                "Removing {} because no IDL implementation for {} was found",
                serviceServerBeanName,
                idlClazz.getCanonicalName());
            beanFactory.removeBeanDefinition(serviceServerBeanName);
          }
        }
      } catch (ClassNotFoundException e) {
        LOGGER.error("Error during post processing of Netifi beans", e);
      }
    }
  }

  private static Class<?> resolveClass(BeanDefinition beanDefinition)
      throws ClassNotFoundException {
    Class<?> clazz = null;

    if (beanDefinition.getBeanClassName() != null) {
      clazz =
          ClassUtils.forName(
              beanDefinition.getBeanClassName(), beanDefinition.getClass().getClassLoader());
    } else if (beanDefinition instanceof AnnotatedBeanDefinition) {
      MethodMetadata metadata =
          ((AnnotatedBeanDefinition) beanDefinition).getFactoryMethodMetadata();

      if (metadata != null) {
        clazz =
            ClassUtils.forName(
                metadata.getReturnTypeName(), beanDefinition.getClass().getClassLoader());
      }
    }

    return clazz;
  }

  private static ResolvableType resolveResolvableType(BeanDefinition beanDefinition) {

    if (beanDefinition instanceof RootBeanDefinition) {
      return ResolvableType.forClass(((RootBeanDefinition) beanDefinition).getTargetType());
    } else if (beanDefinition instanceof GenericBeanDefinition) {
      return ResolvableType.forClass(((GenericBeanDefinition) beanDefinition).getBeanClass());
    } else {
      return beanDefinition.getResolvableType();
    }
  }

  private static boolean findRealImplementationAndMarkAsPrimary(
      DefaultListableBeanFactory beanFactory, Class<?> clazz) {

    for (String beanName : beanFactory.getBeanNamesForType(clazz)) {
      BeanDefinition definition = beanFactory.getBeanDefinition(beanName);

      if (definition instanceof AnnotatedBeanDefinition) {
        AnnotatedBeanDefinition beanDefinition = (AnnotatedBeanDefinition) definition;

        AnnotationMetadata metadata = beanDefinition.getMetadata();

        if (!metadata.hasAnnotation(Generated.class.getName())) {
          definition.setPrimary(true);
          return true;
        }
      } else {
        definition.setPrimary(true);
        return true;
      }
    }

    return false;
  }

  private static DefaultListableBeanFactory unwrapDefaultListableBeanFactory(
      BeanDefinitionRegistry registry) {
    if (registry instanceof DefaultListableBeanFactory) {
      return (DefaultListableBeanFactory) registry;
    } else if (registry instanceof GenericApplicationContext) {
      return ((GenericApplicationContext) registry).getDefaultListableBeanFactory();
    } else {
      return null;
    }
  }
}
