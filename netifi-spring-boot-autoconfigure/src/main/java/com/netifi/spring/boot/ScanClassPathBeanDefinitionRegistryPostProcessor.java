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

import io.rsocket.rpc.annotations.internal.Generated;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

public class ScanClassPathBeanDefinitionRegistryPostProcessor
    implements BeanDefinitionRegistryPostProcessor {

  @Override
  public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry)
      throws BeansException {
    DefaultListableBeanFactory beanFactory = unwrapDefaultListableBeanFactory(registry);
    Environment environment = beanFactory.getBean(Environment.class);

    String[] componentScanAnnotatedBeanNames =
        beanFactory.getBeanNamesForAnnotation(ComponentScan.class);
    Set<String> basePackages = new LinkedHashSet<>();

    for (String beanName : componentScanAnnotatedBeanNames) {
      AnnotatedBeanDefinition definition =
          (AnnotatedBeanDefinition) beanFactory.getBeanDefinition(beanName);
      BeanDefinition mergedBeanDefinition = beanFactory.getMergedBeanDefinition(beanName);
      Map<String, Object> attributes =
          definition.getMetadata().getAnnotationAttributes(ComponentScan.class.getName());
      String[] basePackagesArray = (String[]) attributes.get("basePackages");
      for (String pkg : basePackagesArray) {
        String[] tokenized =
            StringUtils.tokenizeToStringArray(
                environment.resolvePlaceholders(pkg),
                ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS);
        Collections.addAll(basePackages, tokenized);
      }
      for (Class<?> clazz : (Class<?>[]) attributes.get("basePackageClasses")) {
        basePackages.add(ClassUtils.getPackageName(clazz));
      }
      if (basePackages.isEmpty()) {
        basePackages.add(ClassUtils.getPackageName(mergedBeanDefinition.getBeanClassName()));
      }
    }

    ClassPathBeanDefinitionScanner scanner = new ClassPathBeanDefinitionScanner(beanFactory, false);
    scanner.addIncludeFilter(new AnnotationTypeFilter(Generated.class));
    scanner.scan(basePackages.toArray(new String[0]));
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
      throws BeansException {}

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
