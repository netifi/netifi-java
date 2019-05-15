package com.netifi.spring.core.annotation;

import java.util.HashMap;
import java.util.Map;

import com.netifi.common.tags.Tags;
import com.netifi.spring.core.BrokerClientFactory;
import com.netifi.spring.core.BrokerClientFactorySupport;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;

public class BaseBrokerClientFactory<T> implements BrokerClientFactory<T> {

    private final BrokerClientFactorySupport     brokerClientFactorySupport;
    private final BrokerClient.Type              routeType;
    private final String                         destination;
    private final Class<?>                       clientClass;
    private final String                         group;
    private final Tags                           tags;
    private final Map<String, Object>            instantiatedClients;

    public BaseBrokerClientFactory(
        BrokerClientFactorySupport brokerClientFactorySupport,
        BrokerClient.Type routeType,
        String destination,
        Class<T> clientClass,
        String group,
        Tags tags
    ) {
        this.brokerClientFactorySupport = brokerClientFactorySupport;
        this.routeType = routeType;
        this.destination = destination;
        this.clientClass = clientClass;
        this.group = group;
        this.tags = tags;
        this.instantiatedClients = new HashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T lookup(BrokerClient.Type type,
        String methodGroup,
        Tags methodTags) {
        String key = key(type, methodGroup, methodTags);
        T client;
        if (instantiatedClients.containsKey(key)) {
            client = (T) instantiatedClients.get(key);
        }
        else {
            try {
                client = (T) brokerClientFactorySupport.lookup(clientClass, type,
                    methodGroup, methodTags);
                instantiatedClients.put(key, client);
            }
            catch (Exception e) {
                throw new RuntimeException(String.format(
                    "Error instantiating Netifi Broker Client for '%s'",
                    clientClass.getSimpleName()), e);
            }
        }
        return client;
    }

    @Override
    public T lookup(BrokerClient.Type type,
        String methodGroup,
        String... methodTags) {
        return lookup(type, methodGroup, Tags.of(methodTags));
    }

    @Override
    public T lookup(BrokerClient.Type type) {
        return lookup(type, group, tags);
    }

    @Override
    public T lookup(BrokerClient.Type type, Tags methodTags) {
        return lookup(type, group, methodTags);
    }

    @Override
    public T lookup(String group, Tags tags) {
        return lookup(routeType, group, tags);
    }

    @Override
    public T lookup(String group, String... tags) {
        return lookup(routeType, group, tags);
    }

    @Override
    public T lookup(Tags tags) {
        return lookup(routeType, tags);
    }

    @Override
    public T lookup() {
        return lookup(routeType);
    }

    //TODO: Do something smarter
    private String key(BrokerClient.Type type, String group, Tags tags) {
        return type.name() + group + destination + tags.hashCode();
    }
}
