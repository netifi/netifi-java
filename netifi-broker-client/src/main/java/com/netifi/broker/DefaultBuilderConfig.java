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
package com.netifi.broker;

import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import com.typesafe.config.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Gets current default configuration for {@link BrokerFactory}. Can be overridden with System
 * properties, or if the application provides a config file. The builder will over-ride these values
 * if they are based directly in to the builder. Otherwise it will these values a default.
 */
final class DefaultBuilderConfig {
  private static final Config conf = ConfigFactory.load();

  private DefaultBuilderConfig() {}

  static boolean isSslDisabled() {
    return conf.hasPath("netifi.client.ssl.disabled")
        && conf.getBoolean("netifi.client.ssl.disabled");
  }

  static boolean getKeepAlive() {
    boolean keepalive = true;
    try {
      keepalive = conf.getBoolean("netifi.client.keepalive.enable");
    } catch (ConfigException.Missing m) {

    }

    return keepalive;
  }

  static long getTickPeriodSeconds() {
    long tickPeriodSeconds = 20;
    try {
      tickPeriodSeconds = conf.getLong("netifi.client.keepalive.tickPeriodSeconds");
    } catch (ConfigException.Missing m) {

    }

    return tickPeriodSeconds;
  }

  static long getAckTimeoutSeconds() {
    long ackTimeoutSeconds = 30;
    try {
      ackTimeoutSeconds = conf.getLong("netifi.client.keepalive.ackTimeoutSeconds");
    } catch (ConfigException.Missing m) {

    }

    return ackTimeoutSeconds;
  }

  static int getMissedAcks() {
    int missedAcks = 3;
    try {
      missedAcks = conf.getInt("netifi.client.keepalive.missedAcks");
    } catch (ConfigException.Missing m) {
    }

    return missedAcks;
  }

  static InetAddress getLocalAddress() {
    InetAddress localAddress = null;

    try {
      localAddress = InetAddress.getByName(conf.getString("netifi.client.localAddress"));
    } catch (ConfigException.Missing | UnknownHostException m) {

    }

    return localAddress;
  }

  static String getHost() {
    String host = null;
    try {
      host = conf.getString("netifi.client.host");
    } catch (ConfigException.Missing m) {

    }

    return host;
  }

  static int getPort() {
    int port = 8001;
    try {
      port = conf.getInt("netifi.client.port");
    } catch (ConfigException.Missing m) {

    }

    return port;
  }

  static String getGroup() {
    String group = null;
    try {
      group = conf.getString("netifi.client.group");
    } catch (ConfigException.Missing m) {

    }

    return group;
  }

  static String getDestination() {
    String destination = null;
    try {
      destination = conf.getString("netifi.client.destination");
    } catch (ConfigException.Missing m) {

    }

    return destination;
  }

  static short getAdditionalConnectionFlags() {
    // Maybe configure this some day but for now that default is 0.
    return 0;
  }

  static Tags getTags() {
    Tags tags = Tags.empty();
    try {
      Stream<Tag> stream =
          conf.getObject("netifi.client.tags")
              .entrySet()
              .stream()
              .map(
                  e -> {
                    StringBuilder key = new StringBuilder(e.getKey());
                    ConfigValue configValue = e.getValue();

                    while (configValue != null
                        && configValue.valueType() == ConfigValueType.OBJECT) {
                      Set<String> keySet = ((ConfigObject) configValue).keySet();
                      String nextKey = keySet.iterator().next();
                      key.append(".").append(nextKey);
                      configValue = ((ConfigObject) configValue).get(nextKey);
                    }

                    if (configValue != null && configValue.valueType() == ConfigValueType.STRING) {
                      String value = (String) configValue.unwrapped();

                      if (value.isEmpty()) {
                        throw new IllegalArgumentException("Tag mapping " + key + " is empty");
                      }

                      return Tag.of(key.toString(), value);
                    }

                    throw new IllegalArgumentException(
                        "Tag mapping " + key + " is not a string: " + configValue);
                  });
      tags = Tags.of(stream.collect(Collectors.toList()));
    } catch (ConfigException.Missing m) {

    } catch (Throwable t) {
      System.err.println("error parsing tags from config: " + t.getMessage());
    }

    return tags;
  }

  static Long getAccessKey() {
    Long accessKey = null;

    try {
      accessKey = conf.getLong("netifi.client.accessKey");
    } catch (ConfigException.Missing m) {

    }

    return accessKey;
  }

  static String getAccessToken() {
    String accessToken = null;

    try {
      accessToken = conf.getString("netifi.client.accessToken");
    } catch (ConfigException.Missing m) {

    }

    return accessToken;
  }

  static String getConnectionId() {
    String connectionId = null;

    try {
      connectionId = conf.getString("netifi.client.connectionId");
    } catch (ConfigException.Missing m) {

    }

    return connectionId;
  }

  static int getPoolSize() {
    int poolSize = Math.min(4, Runtime.getRuntime().availableProcessors());
    try {
      poolSize = conf.getInt("netifi.client.poolSize");
    } catch (ConfigException.Missing m) {
    }
    return poolSize;
  }

  static int getMinHostsAtStartup() {
    int minHostsAtStartup = 3;
    try {
      minHostsAtStartup = conf.getInt("netifi.client.minHostsAtStartup");
    } catch (ConfigException.Missing m) {
    }
    return minHostsAtStartup;
  }

  static long getMinHostsAtStartupTimeoutSeconds() {
    long minHostsAtStartupTimeout = 5;

    try {
      minHostsAtStartupTimeout = conf.getLong("netifi.client.minHostsAtStartupTimeout");
    } catch (ConfigException.Missing m) {
    }
    return minHostsAtStartupTimeout;
  }

  static String getMetricHandlerGroup() {
    String metricHandlerGroup = "netifi.metrics";
    try {
      metricHandlerGroup = conf.getString("netifi.client.metrics.group");
    } catch (ConfigException.Missing m) {
    }
    return metricHandlerGroup;
  }

  static int getBatchSize() {
    int batchSize = 1_000;

    try {
      batchSize = conf.getInt("netifi.client.metrics.metricBatchSize");
    } catch (ConfigException.Missing m) {
    }
    return batchSize;
  }

  static long getExportFrequencySeconds() {
    long exportFrequencySeconds = 10;

    try {
      exportFrequencySeconds = conf.getLong("netifi.client.metrics.frequency");
    } catch (ConfigException.Missing m) {
    }
    return exportFrequencySeconds;
  }

  static boolean getExportSystemMetrics() {
    boolean exportSystemMetrics = true;

    try {
      exportSystemMetrics = conf.getBoolean("netifi.client.metrics.exportSystemMetrics");
    } catch (ConfigException.Missing m) {
    }
    return exportSystemMetrics;
  }

  static List<InetSocketAddress> getSeedAddress() {
    List<InetSocketAddress> seedAddresses = null;
    try {
      String s = conf.getString("netifi.client.seedAddresses");
      if (s != null) {
        seedAddresses = new ArrayList<>();
        String[] split = s.split(",");
        for (String a : split) {
          String[] split1 = a.split(":");
          if (split1.length == 2) {
            String host = split1[0];
            try {
              int port = Integer.parseInt(split1[1]);
              seedAddresses.add(InetSocketAddress.createUnresolved(host, port));
            } catch (NumberFormatException fe) {
              throw new IllegalStateException("invalid seed address: " + a);
            }
          } else {
            throw new IllegalStateException("invalid seed address: " + a);
          }
        }
      }
    } catch (ConfigException.Missing m) {

    }

    return seedAddresses;
  }
}
