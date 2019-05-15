package com.netifi.spring.messaging;

import com.netifi.broker.BrokerClient;
import com.netifi.broker.rsocket.BrokerSocket;
import com.netifi.broker.rsocket.NamedRSocketClientWrapper;
import com.netifi.common.tags.Tags;
import com.netifi.spring.core.annotation.BrokerClientStaticFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;

import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;

class RSocketRequesterStaticFactory {

    static RSocketRequester createRSocketRequester(
        String rSocketName,
        BrokerClient brokerClient,
        com.netifi.spring.core.annotation.BrokerClient brokerClientAnnotation,
        Tags tags,
        RSocketStrategies rSocketStrategies) {
        return RSocketRequester.create(
            resolveBrokerClientRSocket(
                rSocketName,
                brokerClient,
                brokerClientAnnotation.type(),
                brokerClientAnnotation.group(),
                brokerClientAnnotation.destination(),
                tags
            ),
            null,
            rSocketStrategies
        );
    }

    static RSocketRequester createRSocketRequester(
        String rSocketName,
        BrokerClient brokerClient,
        com.netifi.spring.core.annotation.BrokerClient.Type routingType,
        String group,
        String destination,
        Tags tags,
        RSocketStrategies rSocketStrategies,
        MeterRegistry meterRegistry,
        Tracer tracer
    ) {

        return new MetricsAwareRSocketRequester(
            RSocketRequester.create(
                resolveBrokerClientRSocket(rSocketName, brokerClient, routingType, group, destination, tags),
                null,
                rSocketStrategies
            ),
            meterRegistry,
            tracer
        );
    }

    static BrokerSocket resolveBrokerClientRSocket(
        String rSocketName,
        BrokerClient brokerClient,
        com.netifi.spring.core.annotation.BrokerClient.Type routingType,
        String group,
        String destination,
        Tags tags
    ) {
        return NamedRSocketClientWrapper.wrap(rSocketName, BrokerClientStaticFactory.createBrokerRSocket(
            brokerClient,
            routingType,
            group,
            destination,
            tags
        ));
    }
}
