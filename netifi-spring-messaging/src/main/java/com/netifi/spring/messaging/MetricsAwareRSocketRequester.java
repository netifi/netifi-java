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
package com.netifi.spring.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentracing.Tracer;
import io.rsocket.RSocket;
import io.rsocket.rpc.metrics.Metrics;
import java.util.function.Consumer;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MetricsAwareRSocketRequester implements RSocketRequester {

  private final RSocketRequester rSocketRequester;
  private final MeterRegistry registry;
  private final Tracer tracer;

  public MetricsAwareRSocketRequester(
      RSocketRequester requester,
      RSocketStrategies rSocketStrategies,
      MeterRegistry meterRegistry,
      Tracer tracer) {
    this.rSocketRequester = requester;
    this.registry = meterRegistry;
    this.tracer = tracer;
  }

  @Override
  public RSocket rsocket() {
    return rSocketRequester.rsocket();
  }

  @Override
  public MimeType dataMimeType() {
    return rSocketRequester.dataMimeType();
  }

  @Override
  public MimeType metadataMimeType() {
    return rSocketRequester.metadataMimeType();
  }

  @Override
  public RequestSpec route(String route, Object... routeVars) {
    return new MetricsAwareRequestSpec(
        rSocketRequester.route(route, routeVars), route, registry, tracer);
  }

  @Override
  public RequestSpec metadata(Object metadata, MimeType mimeType) {
    return null;
  }

  private static final class MetricsAwareRequestSpec implements RequestSpec {

    private final RequestSpec requestSpec;
    private final String route;
    private final MeterRegistry registry;
    private final Tracer tracer;

    private MetricsAwareRequestSpec(
        RequestSpec spec, String route, MeterRegistry registry, Tracer tracer) {
      this.requestSpec = spec;
      this.route = route;
      this.registry = registry;
      this.tracer = tracer;
    }

    @Override
    public RequestSpec metadata(Consumer<MetadataSpec<?>> configurer) {
      return null;
    }

    @Override
    public RetrieveSpec data(Object data) {
      requestSpec.data(data);
      return this;
    }

    @Override
    public RetrieveSpec data(Object producer, Class<?> elementClass) {
      requestSpec.data(producer, elementClass);
      return this;
    }

    @Override
    public RetrieveSpec data(Object producer, ParameterizedTypeReference<?> elementTypeRef) {
      requestSpec.data(producer, elementTypeRef);
      return this;
    }

    @Override
    public RequestSpec metadata(Object metadata, MimeType mimeType) {
      return requestSpec;
    }

    @Override
    public Mono<Void> send() {
      return requestSpec
          .send()
          .<Void>transform(
              Metrics.<Void>timed(
                  registry, "rsocket.spring.client", "route", route, "call.type", "send"));
    }

    @Override
    public <T> Mono<T> retrieveMono(Class<T> dataType) {
      return requestSpec
          .retrieveMono(dataType)
          .transform(
              Metrics.<T>timed(
                  registry,
                  "rsocket.spring.client",
                  "route",
                  route,
                  "call.type",
                  "retrieveMono",
                  "data.type",
                  dataType.getName()));
    }

    @Override
    public <T> Mono<T> retrieveMono(ParameterizedTypeReference<T> dataTypeRef) {
      return requestSpec
          .retrieveMono(dataTypeRef)
          .transform(
              Metrics.<T>timed(
                  registry,
                  "rsocket.spring.client",
                  "route",
                  route,
                  "call.type",
                  "retrieveMono",
                  "data.type",
                  dataTypeRef.getType().getTypeName()));
    }

    @Override
    public <T> Flux<T> retrieveFlux(Class<T> dataType) {
      return requestSpec
          .retrieveFlux(dataType)
          .transform(
              Metrics.<T>timed(
                  registry,
                  "rsocket.spring.client",
                  "route",
                  route,
                  "call.type",
                  "retrieveFlux",
                  "data.type",
                  dataType.getName()));
    }

    @Override
    public <T> Flux<T> retrieveFlux(ParameterizedTypeReference<T> dataTypeRef) {
      return requestSpec
          .retrieveFlux(dataTypeRef)
          .transform(
              Metrics.<T>timed(
                  registry,
                  "rsocket.spring.client",
                  "route",
                  route,
                  "call.type",
                  "retrieveFlux",
                  "data.type",
                  dataTypeRef.getType().getTypeName()));
    }
  }
}
