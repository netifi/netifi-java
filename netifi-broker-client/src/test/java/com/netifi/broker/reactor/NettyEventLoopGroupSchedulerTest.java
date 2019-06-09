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
package com.netifi.broker.reactor;

import io.netty.channel.nio.NioEventLoopGroup;
import java.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;

public class NettyEventLoopGroupSchedulerTest {
  static NioEventLoopGroup nioEventLoopGroup;

  @BeforeClass
  public static void setup() {
    nioEventLoopGroup = new NioEventLoopGroup();
  }

  @AfterClass
  public static void teardown() {
    try {
      nioEventLoopGroup.shutdownGracefully().get();
    } catch (Exception t) {
      throw new RuntimeException(t);
    }
  }

  @Test
  public void testSchedule() {
    NettyEventLoopGroupScheduler scheduler = new NettyEventLoopGroupScheduler(nioEventLoopGroup);

    Flux.interval(Duration.ofMillis(100), scheduler)
        .take(Duration.ofSeconds(1), scheduler)
        .blockLast();
  }
}
