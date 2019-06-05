package com.netifi.spring.core.annotation;

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BrokerClientFactory;
import com.netifi.spring.core.DestinationAwareClientFactory;

class DefaultDestinationAwareClientFactory<T> implements DestinationAwareClientFactory<T> {

  private final BrokerClientFactory<T> baseFactory;

  DefaultDestinationAwareClientFactory(BrokerClientFactory<T> factory) {
    baseFactory = factory;
  }

  @Override
  public T lookup(BrokerClient.Type type, String group, Tags tags) {
    return baseFactory.lookup(type, group, tags);
  }

  @Override
  public T lookup(BrokerClient.Type type, String group, String... tags) {
    return baseFactory.lookup(type, group, tags);
  }

  @Override
  public T lookup(BrokerClient.Type type) {
    return baseFactory.lookup(type);
  }

  @Override
  public T lookup(BrokerClient.Type type, Tags tags) {
    return baseFactory.lookup(type, tags);
  }

  @Override
  public T lookup(String group, Tags tag) {
    return baseFactory.lookup(group, tag);
  }

  @Override
  public T lookup(String group, String... tags) {
    return baseFactory.lookup(group, tags);
  }

  @Override
  public T lookup(Tags tags) {
    return baseFactory.lookup(tags);
  }

  @Override
  public T lookup() {
    return baseFactory.lookup();
  }
}
