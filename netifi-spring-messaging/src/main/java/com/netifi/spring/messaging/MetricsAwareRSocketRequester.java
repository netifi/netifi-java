package com.netifi.spring.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.RSocket;
import io.rsocket.rpc.metrics.Metrics;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.rsocket.RSocketRequester;

public class MetricsAwareRSocketRequester implements RSocketRequester {

    private final RSocketRequester rSocketRequester;
    private final MeterRegistry registry;
    private final Tracer tracer;

    public MetricsAwareRSocketRequester(
        RSocketRequester requester,
        MeterRegistry meterRegistry,
        Tracer tracer
    ) {
        this.rSocketRequester = requester;
        this.registry = meterRegistry;
        this.tracer = tracer;
    }

    @Override
    public RSocket rsocket() {
        return rSocketRequester.rsocket();
    }

    @Override
    public RequestSpec route(String route) {
        return new MetricsAwareRequestSpec(rSocketRequester.route(route), route,
            registry,
            tracer);
    }

    private static final class MetricsAwareRequestSpec implements RequestSpec {

        private final RequestSpec requestSpec;
        private final String      route;
        private final MeterRegistry registry;
        private final Tracer tracer;

        private MetricsAwareRequestSpec(RequestSpec spec,
            String route,
            MeterRegistry registry,
            Tracer tracer) {
            this.requestSpec = spec;
            this.route = route;
            this.registry = registry;
            this.tracer = tracer;
        }

        @Override
        public ResponseSpec data(Object data) {
            return requestSpec.data(data);
        }

        @Override
        public <T, P extends Publisher<T>> ResponseSpec data(P publisher, Class<T> dataType) {
            return requestSpec.data(publisher, dataType);
        }

        @Override
        public <T, P extends Publisher<T>> ResponseSpec data(P publisher, ParameterizedTypeReference<T> dataTypeRef) {
            return requestSpec.data(publisher, dataTypeRef);
        }
    }

    private static final class MetricsAwareResponseSepc implements ResponseSpec {

        private final ResponseSpec responseSpec;
        private final String       route;
        private final MeterRegistry registry;
        private final Tracer tracer;

        private MetricsAwareResponseSepc(ResponseSpec spec,
            String route,
            MeterRegistry registry,
            Tracer tracer) {
            this.responseSpec = spec;
            this.route = route;
            this.registry = registry;
            this.tracer = tracer;
        }

        @Override
        public Mono<Void> send() {
            return responseSpec
                .send()
                .<Void>transform(Metrics.<Void>timed(
                    registry, "rsocket.spring.client",
                    "route", route,
                    "call.type", "send"
                ));
        }

        @Override
        public <T> Mono<T> retrieveMono(Class<T> dataType) {
            return responseSpec
                .retrieveMono(dataType)
                .<T>transform(Metrics.<T>timed(
                    registry, "rsocket.spring.client",
                    "route", route,
                    "call.type", "retrieveMono",
                    "data.type", dataType.getName()
                ));
        }

        @Override
        public <T> Mono<T> retrieveMono(ParameterizedTypeReference<T> dataTypeRef) {
            return responseSpec
                .retrieveMono(dataTypeRef)
                .<T>transform(Metrics.<T>timed(
                    registry, "rsocket.spring.client",
                    "route", route,
                    "call.type", "retrieveMono",
                    "data.type", dataTypeRef.getType().getTypeName()
                ));
        }

        @Override
        public <T> Flux<T> retrieveFlux(Class<T> dataType) {
            return responseSpec
                .retrieveFlux(dataType)
                .<T>transform(Metrics.<T>timed(
                    registry, "rsocket.spring.client",
                    "route", route,
                    "call.type", "retrieveFlux",
                    "data.type", dataType.getName()
                ));
        }

        @Override
        public <T> Flux<T> retrieveFlux(ParameterizedTypeReference<T> dataTypeRef) {
            return responseSpec
                .retrieveFlux(dataTypeRef)
                .<T>transform(Metrics.<T>timed(
                    registry, "rsocket.spring.client",
                    "route", route,
                    "call.type", "retrieveFlux",
                    "data.type", dataTypeRef.getType().getTypeName()
                ));
        }
    }
}
