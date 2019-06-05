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
package com.netifi.spring.boot.test;

import com.netifi.spring.core.annotation.Group;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class SpringMessagingIntegrationTest {

  public static GenericContainer redis =
      new GenericContainer("netifi/broker:1.6.4")
          .withExposedPorts(8001, 7001, 6001, 8101)
          .withEnv(
              "BROKER_SERVER_OPTS",
              "-Dnetifi.authentication.0.accessKey=9007199254740991 "
                  + "-Dnetifi.authentication.0.accessToken=kTBDVtfRBO4tHOnZzSyY5ym2kfY= "
                  + "-Dnetifi.broker.admin.accessKey=9007199254740991 "
                  + "-Dnetifi.broker.admin.accessToken=kTBDVtfRBO4tHOnZzSyY5ym2kfY=");

  static {
    redis.start();
    System.setProperty("netifi.client.broker.hostname", redis.getContainerIpAddress());
    System.setProperty("netifi.client.broker.port", String.valueOf(redis.getMappedPort(8001)));
  }

  @Group("com.netifi.client.demo.vowelcount")
  RSocketRequester requester;

  @Test
  public void tests() {
    Assert.assertNotNull(requester.rsocket());

    requester.route("test.process").data("test").retrieveMono(String.class).log().block();
  }
}
