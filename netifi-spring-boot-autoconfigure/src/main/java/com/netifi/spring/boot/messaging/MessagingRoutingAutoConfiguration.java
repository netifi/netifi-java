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
package com.netifi.spring.boot.messaging;

import com.netifi.spring.boot.BrokerClientAutoConfiguration;
import com.netifi.spring.boot.DefaultRoutingAutoConfiguration;
import com.netifi.spring.messaging.MessagingRouter;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.util.MimeTypeUtils;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({RSocketRequester.class, RSocketFactory.class, TcpServerTransport.class})
@AutoConfigureBefore({BrokerClientAutoConfiguration.class, DefaultRoutingAutoConfiguration.class})
@AutoConfigureAfter(RSocketStrategiesAutoConfiguration.class)
public class MessagingRoutingAutoConfiguration {

  @Bean
  public MessagingRouter messagingRouter(
      RSocketStrategies rSocketStrategies, RSocketMessageHandler handler) {
    return new MessagingRouter(
        MimeTypeUtils.ALL,
        MimeTypeUtils.ALL,
        rSocketStrategies.metadataExtractor(),
        handler,
        rSocketStrategies.routeMatcher(),
        rSocketStrategies);
  }
}
