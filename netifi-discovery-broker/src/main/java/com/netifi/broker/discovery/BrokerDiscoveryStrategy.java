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
package com.netifi.broker.discovery;

import com.netifi.broker.BrokerClient;
import com.netifi.broker.info.BrokerInfoServiceClient;
import com.netifi.broker.info.Destination;
import com.netifi.broker.info.Tag;
import com.netifi.broker.info.Tags;
import com.netifi.broker.rsocket.BrokerSocket;
import com.netifi.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BrokerDiscoveryStrategy implements DiscoveryStrategy, Disposable {
  private final Logger logger = LoggerFactory.getLogger(BrokerDiscoveryStrategy.class);
  private final BrokerClient client;

  private final BrokerInfoServiceClient infoServiceClient;

  private final Tags tags;

  private final ReplayProcessor<Map<String, HostAndPort>> knownBrokers;

  private final Disposable disposable;

  public BrokerDiscoveryStrategy(BrokerDiscoveryConfig config) {
    this.client = config.getClient();
    this.knownBrokers = ReplayProcessor.cacheLast();

    BrokerSocket brokerSocket =
        client.groupServiceSocket(
            config.getBrokerInfoServiceGroup(), config.getBrokerInfoServiceTags());

    this.infoServiceClient = new BrokerInfoServiceClient(brokerSocket);
    Tag.Builder tag =
        Tag.newBuilder()
            .setKey("netifi.broker.cluster.clusterName")
            .setValue(config.getClusterName());
    this.tags = Tags.newBuilder().addTags(tag).build();
    this.disposable =
        infoServiceClient
            .streamDestinationRollupEventsFilteredByTags(tags)
            .scan(
                new ConcurrentHashMap<String, HostAndPort>(),
                (map, event) -> {
                  Destination destination = event.getDestination();
                  List<Tag> tagsList = destination.getTagsList();
                  Tag brokerId = findTag(tagsList, "netifi.broker.brokerId");
                  Tag brokerName = findTag(tagsList, "netifi.broker.brokerName");

                  switch (event.getType()) {
                    case JOIN:
                      {
                        logger.info(
                            "adding broker with id {} - named {} - \n{}",
                            brokerId.getValue(),
                            brokerName.getValue(),
                            event);

                        Tag address = findTag(tagsList, config.getAddressTagKey());
                        Tag port = findTag(tagsList, config.getPortTagKey());

                        HostAndPort hp =
                            HostAndPort.fromParts(
                                address.getValue(), Integer.parseInt(port.getValue()));

                        map.put(brokerId.getValue(), hp);
                      }
                      break;
                    case LEAVE:
                      {
                        logger.info(
                            "removing broker with id {} - named {}",
                            brokerId.getValue(),
                            brokerName.getValue());
                        map.remove(brokerId.getValue());
                      }
                      break;
                    default:
                      throw new IllegalStateException("unknown event");
                  }

                  return map;
                })
            .sample(Duration.ofMillis(250))
            .doOnError(
                throwable ->
                    logger.error(
                        "error streaming destination events filtered by tag key {} with value {}",
                        tag.getKey(),
                        tag.getValue()))
            .retryBackoff(Integer.MAX_VALUE, Duration.ofSeconds(1), Duration.ofSeconds(30))
            .subscribe(knownBrokers::onNext);
  }

  @Override
  public void dispose() {
    disposable.dispose();
  }

  @Override
  public boolean isDisposed() {
    return disposable.isDisposed();
  }

  @Override
  public Mono<? extends Collection<HostAndPort>> discoverNodes() {
    return knownBrokers.next().map(Map::values);
  }

  private static Tag findTag(List<Tag> tags, String name) {
    return tags.stream().filter(tag -> tag.getKey().equals(name)).findFirst().get();
  }
}
