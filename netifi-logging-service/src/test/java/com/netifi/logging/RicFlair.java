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
package com.netifi.logging;

import com.netifi.broker.BrokerClient;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class RicFlair {

  private static long accessKey = Long.getLong("ACCESS_KEY", 9007199254740991L);
  private static String accessToken =
      System.getProperty("ACCESS_TOKEN", "kTBDVtfRBO4tHOnZzSyY5ym2kfY=");

  private static final Logger logger = LogManager.getLogger(RicFlair.class);

  private static final Duration duration = Duration.ofMillis(1_000);

  private static final String[] messages =
      new String[] {
        "I'm Ric Flair! The stylin', profilin', limousine riding, jet flying, kiss-stealing, wheelin' and dealin' son of a gun.",
        "To be the man, you gotta beat the man.",
        "If you don’t like it, learn to love it.",
        "Diamonds are forever and so is Ric Flair.",
        "Wooooooooo!",
        "This ain’t no garden party, brother, this is wrestling, where only the strongest survive.",
        "You’re talking to the Rolex wearin’, diamond ring wearin’, kiss stealin’, wheelin’ dealin’, limousine ridin’, jet flyin’ son of a gun. And I’m having a hard time holding these alligators down. Woo!"
      };

  private static final Level[] levels =
      new Level[] {
        Level.DEBUG,
        Level.DEBUG,
        Level.DEBUG,
        Level.DEBUG,
        Level.DEBUG,
        Level.DEBUG,
        Level.DEBUG,
        Level.DEBUG,
        Level.ERROR,
        Level.ERROR,
        Level.ERROR,
        Level.FATAL,
        Level.WARN,
        Level.INFO,
        Level.INFO,
        Level.INFO,
        Level.INFO,
        Level.INFO,
        Level.INFO,
        Level.INFO,
        Level.INFO,
        Level.INFO
      };

  public static void main(String... args) {

    BrokerClient client =
        BrokerClient.tcp()
            .host("localhost")
            .accessKey(accessKey)
            .accessToken(accessToken)
            .group("TestLogger")
            .build();

    client.addService(
        new LoggingServiceServer(
            new LoggingServiceImpl(Schedulers.newSingle("logger-schduler")),
            Optional.empty(),
            Optional.empty()));

    Flux.interval(duration)
        .doOnNext(
            l -> {
              Level level = getLevel();
              String message = getMessage();
              logger.log(level, message);
            })
        .blockLast();
  }

  private static Level getLevel() {
    int i = ThreadLocalRandom.current().nextInt(levels.length);
    return levels[i];
  }

  private static String getMessage() {
    int i = ThreadLocalRandom.current().nextInt(messages.length);
    return messages[i];
  }
}
