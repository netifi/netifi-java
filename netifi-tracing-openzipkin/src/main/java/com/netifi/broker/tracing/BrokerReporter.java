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
package com.netifi.broker.tracing;

import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import zipkin2.Annotation;
import zipkin2.Component;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

class BrokerReporter extends Component implements Reporter<Span> {
  private static final Logger logger = LoggerFactory.getLogger(BrokerReporter.class);
  private final FluxProcessor<Span, Span> sink;
  private final Disposable disposable;
  private final String group;
  private final Tags tags;

  BrokerReporter(BrokerTracingServiceClient service, String group, Tags tags) {
    this.sink = DirectProcessor.<Span>create().serialize();
    this.group = group;
    this.tags = tags;
    AtomicInteger count = new AtomicInteger();
    AtomicLong lastRetry = new AtomicLong(System.currentTimeMillis());
    this.disposable =
        Flux.defer(() -> service.streamSpans(sink.onBackpressureLatest().map(this::mapSpan)))
            .onErrorResume(
                throwable -> {
                  if (System.currentTimeMillis() - lastRetry.getAndSet(System.currentTimeMillis())
                      > 30_000) {
                    count.set(0);
                  }

                  int i = Math.min(30, count.incrementAndGet());
                  logger.error("error sending tracing data", throwable);
                  return Mono.delay(Duration.ofSeconds(i)).then(Mono.error(throwable));
                })
            .retry()
            .subscribe();
  }

  private zipkin2.proto3.Span mapSpan(Span span) {
    zipkin2.proto3.Span.Builder builder =
        zipkin2.proto3.Span.newBuilder().setName(span.name()).setTraceId(span.traceId());

    if (span.parentId() != null) {
      builder.setParentId(span.parentId());
    }

    builder.setId(span.id());

    if (span.kind() != null) {
      builder.setKind(zipkin2.proto3.Span.Kind.valueOf(span.kind().name()));
    }

    builder.setTimestamp(span.timestampAsLong()).setDuration(span.durationAsLong());

    if (span.localEndpoint() != null) {
      builder.setLocalEndpoint(mapEndpoint(span.localEndpoint()));
    }

    if (span.remoteEndpoint() != null) {
      builder.setRemoteEndpoint(mapEndpoint(span.remoteEndpoint()));
    }

    for (Annotation annotation : span.annotations()) {
      builder.addAnnotations(mapAnnotation(annotation));
    }

    builder
        .putAllTags(span.tags())
        .setDebug(span.debug() == null ? false : span.debug())
        .setShared(span.shared() == null ? false : span.shared())
        .putTags("group", group);

    for (Tag tag : tags) {
      builder.putTags(tag.getKey(), tag.getValue());
    }

    return builder.build();
  }

  private zipkin2.proto3.Endpoint.Builder mapEndpoint(Endpoint endpoint) {
    zipkin2.proto3.Endpoint.Builder builder =
        zipkin2.proto3.Endpoint.newBuilder().setServiceName(group);

    if (endpoint.ipv4() != null) {
      builder.setIpv4(endpoint.ipv4());
    }

    if (endpoint.ipv6() != null) {
      builder.setIpv6(endpoint.ipv6());
    }

    return builder.setPort(endpoint.portAsInt());
  }

  private zipkin2.proto3.Annotation.Builder mapAnnotation(Annotation annotation) {
    return zipkin2.proto3.Annotation.newBuilder()
        .setTimestamp(annotation.timestamp())
        .setValue(annotation.value());
  }

  @Override
  public void report(Span span) {
    if (!sink.isDisposed()) {
      logger.trace("reporting tracing data - {}", span);
      sink.onNext(span);
    }
  }

  @Override
  public void close() throws IOException {
    if (!disposable.isDisposed()) {
      disposable.dispose();
    }
  }
}
