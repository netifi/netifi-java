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
package com.netifi.spring.boot.messaging;

import com.netifi.broker.BrokerService;
import java.net.InetSocketAddress;
import org.springframework.boot.rsocket.context.RSocketServerBootstrap;
import org.springframework.boot.rsocket.context.RSocketServerInitializedEvent;
import org.springframework.boot.rsocket.server.RSocketServer;
import org.springframework.boot.rsocket.server.RSocketServerException;
import org.springframework.context.ApplicationEventPublisher;

public class NetifiBootstrap extends RSocketServerBootstrap {

  private final BrokerService brokerClient;
  private ApplicationEventPublisher eventPublisher;

  public NetifiBootstrap(BrokerService client) {
    super((__) -> null, (setup, sendingSocket) -> null);
    brokerClient = client;
  }

  @Override
  public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
    this.eventPublisher = applicationEventPublisher;
  }

  @Override
  public void start() {
    this.eventPublisher.publishEvent(
        new RSocketServerInitializedEvent(
            new RSocketServer() {
              @Override
              public void start() throws RSocketServerException {}

              @Override
              public void stop() throws RSocketServerException {
                brokerClient.dispose();
              }

              @Override
              public InetSocketAddress address() {
                return InetSocketAddress.createUnresolved("localhost", 0);
              }
            }));
  }

  @Override
  public void stop() {
    this.brokerClient.dispose();
  }

  @Override
  public boolean isRunning() {
    return !brokerClient.isDisposed();
  }
}
