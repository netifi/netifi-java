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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import reactor.core.Disposable;

abstract class Padding0 {
  static final int DISPOSED = 1;
  static final int ACTIVE = 0;
  @SuppressWarnings("unused")
  private long p0, p1, p2, p3, p4, p5, p6;
}

abstract class Padding1 extends Padding0 {
  protected static final AtomicIntegerFieldUpdater<Padding1> STATUS =
      AtomicIntegerFieldUpdater.newUpdater(Padding1.class, "status");
  volatile int status = ACTIVE;
  @SuppressWarnings("unused")
  private long p0, p1, p2, p3, p4, p5, p6;
}

class BooleanDisposable extends Padding1 implements Disposable {
  private final BooleanDisposable booleanDisposable;

  public BooleanDisposable(BooleanDisposable booleanDisposable) {
    this.booleanDisposable = booleanDisposable;
  }

  public BooleanDisposable() {
    booleanDisposable = null;
  }

  @Override
  public boolean isDisposed() {
    return booleanDisposable == null
        ? status == DISPOSED
        : status == DISPOSED && booleanDisposable.isDisposed();
  }

  @Override
  public void dispose() {
    STATUS.compareAndSet(this, ACTIVE, DISPOSED);
  }
}
