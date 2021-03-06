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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.netty.handler.codec.json.JsonObjectDecoder;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import zipkin2.proto3.Span;

public class TracesStreamer {

  private final ObjectMapper objectMapper = protoMapper();
  private Function<Integer, Publisher<InputStream>> inputSource;

  public TracesStreamer(String zipkinUrl, Mono<HttpClient> client) {
    this(zipkinServerStream(zipkinUrl, client));
  }

  public TracesStreamer(Publisher<InputStream> tracesSource) {
    this(v -> tracesSource);
  }

  private TracesStreamer(Function<Integer, Publisher<InputStream>> inputSource) {
    this.inputSource = inputSource;
  }

  public Flux<Trace> streamTraces(int lookbackSeconds) {
    return streamTraces(inputSource.apply(lookbackSeconds));
  }

  Flux<Trace> streamTraces(Publisher<InputStream> input) {
    return Flux.from(input)
        .filter(
            is -> {
              try {
                return is.available() > 0;
              } catch (IOException e) {
                throw Exceptions.propagate(e);
              }
            })
        .map(
            is -> {
              try {
                return objectMapper.readValue(is, new TypeReference<Trace>() {});
              } catch (IOException e) {
                throw Exceptions.propagate(e);
              }
            });
  }

  private static Function<Integer, Publisher<InputStream>> zipkinServerStream(
      String zipkinUrl, Mono<HttpClient> client) {
    return lookbackSeconds ->
        client.flatMapMany(
            c ->
                c.doOnRequest(
                        (__, connection) -> connection.addHandler(new JsonObjectDecoder(true)))
                    .get()
                    .uri(zipkinQuery(zipkinUrl, lookbackSeconds))
                    .responseContent()
                    .asInputStream());
  }

  private static String zipkinQuery(String zipkinUrl, int lookbackSeconds) {
    long lookbackMillis = TimeUnit.SECONDS.toMillis(lookbackSeconds);
    return zipkinUrl + "?lookback=" + lookbackMillis + "&limit=100000";
  }

  private ObjectMapper protoMapper() {
    ObjectMapper mapper = new ObjectMapper();
    ProtobufModule module = new CustomProtoModule();
    mapper.registerModule(module);
    return mapper;
  }

  public static class CustomProtoModule extends ProtobufModule {
    @Override
    public void setupModule(SetupContext context) {
      super.setupModule(context);
      SimpleDeserializers deser = new SimpleDeserializers();
      deser.addDeserializer(Trace.class, new TracersDeserializer());
      context.addDeserializers(deser);
    }
  }

  public static class TracersDeserializer extends StdDeserializer<Trace> {

    public TracersDeserializer() {
      this(null);
    }

    protected TracersDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public Trace deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
      Trace.Builder traceBuilder = Trace.newBuilder();
      while (p.nextToken() != JsonToken.END_ARRAY) {
        traceBuilder.addSpans(ctx.readValue(p, Span.class));
      }
      return traceBuilder.build();
    }
  }
}
