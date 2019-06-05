package com.netifi.spring.core;

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.annotation.BrokerClient;

public interface BrokerClientFactorySupport {

  boolean support(Class<?> clazz);

  <T> T lookup(Class<T> tClass, BrokerClient.Type type, String group, Tags tag);
}
