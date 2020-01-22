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

import com.netifi.common.tags.Tags;
import io.rsocket.RSocket;
import io.rsocket.ipc.MutableRouter;
import reactor.core.publisher.Mono;

class DefaultRoutingBrokerService implements RoutingBrokerService, InstanceInfoAware {

  private final MutableRouter router;
  private final DefaultBrokerService brokerService;

  public DefaultRoutingBrokerService(MutableRouter router, DefaultBrokerService brokerService) {
    this.router = router;
    this.brokerService = brokerService;
  }

  @Override
  public String groupName() {
    return brokerService.groupName();
  }

  @Override
  public Tags tags() {
    return brokerService.tags();
  }

  @Override
  public long accessKey() {
    return brokerService.accessKey();
  }

  @Override
  public MutableRouter router() {
    return router;
  }

  @Override
  public RSocket selectRSocket() {
    return brokerService.selectRSocket();
  }

  @Override
  public Mono<Void> onClose() {
    return brokerService.onClose();
  }

  @Override
  public void dispose() {
    brokerService.dispose();
  }

  @Override
  public boolean isDisposed() {
    return brokerService.isDisposed();
  }
}
