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

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

public class NettyEventLoopGroupScheduler implements Scheduler {
  private EventLoopGroup eventLoopGroup;

  public NettyEventLoopGroupScheduler(EventLoopGroup eventLoopGroup) {
    this.eventLoopGroup = eventLoopGroup;
  }

  private static Disposable doSchedule(
      Runnable task, ExecutorService executorService, BooleanDisposable disposable) {
    executorService.execute(
        () -> {
          if (!disposable.isDisposed()) {
            task.run();
          }
        });
    return disposable;
  }

  private static Disposable doSchedule(
      Runnable task,
      long delay,
      TimeUnit unit,
      ScheduledExecutorService executorService,
      BooleanDisposable disposable) {
    executorService.schedule(
        () -> {
          if (!disposable.isDisposed()) {
            task.run();
          }
        },
        delay,
        unit);
    return disposable;
  }

  private static Disposable doSchedulePeriodically(
      Runnable task,
      long initialDelay,
      long period,
      TimeUnit unit,
      ScheduledExecutorService executorService,
      BooleanDisposable disposable) {
    executorService.scheduleAtFixedRate(
        () -> {
          if (!disposable.isDisposed()) {
            task.run();
          }
        },
        initialDelay,
        period,
        unit);
    return disposable;
  }

  @Override
  public Disposable schedule(Runnable task) {
    BooleanDisposable disposable = new BooleanDisposable();
    return doSchedule(task, eventLoopGroup, disposable);
  }

  @Override
  public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
    BooleanDisposable disposable = new BooleanDisposable();
    return doSchedule(task, delay, unit, eventLoopGroup, disposable);
  }

  @Override
  public Disposable schedulePeriodically(
      Runnable task, long initialDelay, long period, TimeUnit unit) {
    BooleanDisposable disposable = new BooleanDisposable();
    return doSchedulePeriodically(task, initialDelay, period, unit, eventLoopGroup, disposable);
  }

  @Override
  public Worker createWorker() {
    return new InnerWorker(eventLoopGroup.next());
  }

  class InnerWorker implements Worker {
    private final EventLoop eventLoop;
    private final BooleanDisposable disposable;

    public InnerWorker(EventLoop eventLoop) {
      this.eventLoop = eventLoop;
      this.disposable = new BooleanDisposable();
    }

    @Override
    public Disposable schedule(Runnable task) {
      return doSchedule(task, eventLoop, disposable);
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
      return doSchedule(task, delay, unit, eventLoop, disposable);
    }

    @Override
    public Disposable schedulePeriodically(
        Runnable task, long initialDelay, long period, TimeUnit unit) {
      return doSchedulePeriodically(task, initialDelay, period, unit, eventLoop, disposable);
    }

    @Override
    public void dispose() {
      disposable.dispose();
    }

    @Override
    public boolean isDisposed() {
      return disposable.isDisposed();
    }
  }
}
