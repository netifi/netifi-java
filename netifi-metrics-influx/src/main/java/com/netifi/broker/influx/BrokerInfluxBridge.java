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
package com.netifi.broker.influx;

import com.google.common.util.concurrent.AtomicDouble;
import com.netifi.broker.BrokerFactory;
import com.netifi.broker.RoutingBrokerService;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.rsocket.rpc.metrics.om.MeterId;
import io.rsocket.rpc.metrics.om.MeterMeasurement;
import io.rsocket.rpc.metrics.om.MeterTag;
import io.rsocket.rpc.metrics.om.MeterType;
import io.rsocket.rpc.metrics.om.MetricsSnapshot;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandler;
import io.rsocket.rpc.metrics.om.MetricsSnapshotHandlerServer;
import io.rsocket.rpc.metrics.om.Skew;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleConsumer;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Named("BrokerInfluxBridge")
public class BrokerInfluxBridge implements MetricsSnapshotHandler {
  private static final Logger logger = LoggerFactory.getLogger(BrokerInfluxBridge.class);
  private final long metricsSkewInterval;
  private final InfluxMeterRegistry registry;
  ConcurrentHashMap<Meter.Id, DoubleConsumer> consumers = new ConcurrentHashMap<>();

  @Inject
  public BrokerInfluxBridge(Optional<Long> metricsSkewInterval, InfluxMeterRegistry registry) {
    this.metricsSkewInterval = metricsSkewInterval.orElse(10_000L);
    this.registry = registry;
  }

  public static void main(String... args) {
    logger.info("Starting Stand-alone BrokerClient Influx Bridge");

    String group = System.getProperty("netifi.metricsGroup", "com.netifi.broker.metrics");
    String brokerHost = System.getProperty("netifi.host", "localhost");
    int brokerPort = Integer.getInteger("netifi.port", 8001);

    String influxDb = System.getProperty("netifi.influx.db");
    String influxUserName = System.getProperty("netifi.influx.userName");
    String influxPassword = System.getProperty("netifi.influx.password");
    String uri = System.getProperty("netifi.influx.uri");
    String influxRetentionDuration = System.getProperty("netifi.influx.retention", "2w");

    long accessKey = Long.getLong("netifi.accessKey", 3855261330795754807L);
    String accessToken =
        System.getProperty("netifi.authentication.accessToken", "kTBDVtfRBO4tHOnZzSyY5ym2kfY");

    logger.info("group - {}", group);
    logger.info("broker host - {}", brokerHost);
    logger.info("broker port - {}", brokerPort);
    logger.info("access key - {}", accessKey);

    RoutingBrokerService brokerClient =
        BrokerFactory.connect()
            .connection(spec -> spec.tcp())
            .authentication(spec -> spec.simple().key(accessKey).token(accessToken))
            .destinationInfo(spec -> spec.groupName(group).destinationTag("standaloneInfluxBridge"))
            .discoveryStrategy(spec -> spec.simple(brokerPort, brokerHost))
            .toRoutingService();

    InfluxConfig config =
        new InfluxConfig() {
          @Override
          public String get(String key) {
            return null;
          }

          @Override
          public String db() {
            return influxDb;
          }

          @Override
          public String userName() {
            return influxUserName;
          }

          @Override
          public String password() {
            return influxPassword;
          }

          @Override
          public String uri() {
            return uri;
          }

          public String retentionDuration() {
            return influxRetentionDuration;
          }
        };

    AtomicLong influxThreadCount = new AtomicLong();
    new MetricsSnapshotHandlerServer(
            new BrokerInfluxBridge(
                Optional.empty(),
                new InfluxMeterRegistry(
                    config,
                    Clock.SYSTEM,
                    r -> {
                      Thread t = new Thread(r);
                      t.setDaemon(true);
                      t.setName("influx-db-" + influxThreadCount.incrementAndGet());
                      return t;
                    })),
            Optional.empty(),
            Optional.empty(),
            Optional.empty())
        .selfRegister(brokerClient.router());

    brokerClient.onClose().block();
  }

  @Override
  public Flux<Skew> streamMetrics(Publisher<MetricsSnapshot> messages, ByteBuf metadata) {
    Mono<Void> mainFlow =
        Flux.from(messages)
            .limitRate(256, 32)
            .flatMapIterable(MetricsSnapshot::getMetersList)
            .flatMap(
                meter ->
                    Flux.fromIterable(meter.getMeasureList())
                        .doOnNext(meterMeasurement -> record(meter, meterMeasurement)))
            .then();

    return Flux.interval(Duration.ofSeconds(metricsSkewInterval))
        .map(l -> Skew.newBuilder().setTimestamp(System.currentTimeMillis()).build())
        .onBackpressureDrop()
        .takeUntilOther(mainFlow);
  }

  private void record(io.rsocket.rpc.metrics.om.Meter meter, MeterMeasurement meterMeasurement) {
    try {
      MeterId id = meter.getId();
      Iterable<Tag> tags = mapTags(id.getTagList());
      String name = id.getName();
      MeterType type = meter.getId().getType();
      String baseUnit = id.getBaseUnit();
      String description = id.getDescription();

      switch (type) {
        case GAUGE:
          consumers
              .computeIfAbsent(
                  new Meter.Id(name, tags, baseUnit, description, Meter.Type.GAUGE),
                  i -> {
                    AtomicDouble holder = new AtomicDouble();
                    registry.gauge(generateInfluxDbFriendNames(i), tags, holder);
                    return holder::set;
                  })
              .accept(meterMeasurement.getValue());
          break;
        case LONG_TASK_TIMER:
        case TIMER:
          consumers
              .computeIfAbsent(
                  new Meter.Id(name, tags, baseUnit, description, Meter.Type.TIMER),
                  i ->
                      new DoubleConsumer() {
                        Timer timer = registry.timer(generateInfluxDbFriendNames(i), tags);

                        @Override
                        public void accept(double value) {
                          timer.record(Duration.ofNanos((long) value));
                        }
                      })
              .accept(meterMeasurement.getValue());
          break;
        case COUNTER:
          consumers
              .computeIfAbsent(
                  new Meter.Id(name, tags, baseUnit, description, Meter.Type.COUNTER),
                  i ->
                      new DoubleConsumer() {
                        Counter counter = registry.counter(generateInfluxDbFriendNames(i), tags);

                        @Override
                        public void accept(double value) {
                          counter.increment(value);
                        }
                      })
              .accept(meterMeasurement.getValue());
          break;
        case DISTRIBUTION_SUMMARY:
          consumers
              .computeIfAbsent(
                  new Meter.Id(name, tags, baseUnit, description, Meter.Type.DISTRIBUTION_SUMMARY),
                  i ->
                      new DoubleConsumer() {

                        DistributionSummary counter =
                            DistributionSummary.builder(generateInfluxDbFriendNames(i))
                                .tags(i.getTags())
                                .baseUnit(i.getBaseUnit())
                                .description(description)
                                .register(registry);

                        @Override
                        public void accept(double value) {
                          counter.record(value);
                        }
                      })
              .accept(meterMeasurement.getValue());
          break;
        default:
      }
    } catch (Throwable t) {
      logger.debug("error recording metric for " + meter.getId().getName(), t);
    }
  }

  List<Tag> mapTags(List<MeterTag> tags) {
    return tags.stream()
        .map(meterTag -> Tag.of(meterTag.getKey(), meterTag.getValue()))
        .collect(Collectors.toList());
  }

  String generateInfluxDbFriendNames(Meter.Id id) {
    return id.getName();
  }

  private Optional<Tag> findTagByKey(Meter.Id id, String key) {
    return id.getTags().stream().filter(tag -> tag.getKey().equals(key)).findFirst();
  }
}
