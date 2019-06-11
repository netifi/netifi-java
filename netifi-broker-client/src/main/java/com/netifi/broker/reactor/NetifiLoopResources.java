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

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.netty.resources.LoopResources;

public class NetifiLoopResources implements LoopResources {
  private static class LazyHolder2 {
    private static final NetifiLoopResources INSTANCE = new NetifiLoopResources();
  }

  public static NetifiLoopResources getInstance() {
    return LazyHolder2.INSTANCE;
  }

  @Override
  public EventLoopGroup onServer(boolean useNative) {
    return LazyHolder.getInstance().eventLoopGroup;
  }

  @Override
  public Class<? extends Channel> onChannel(EventLoopGroup group) {
    return LazyHolder.getInstance().channel;
  }

  @Override
  public Class<? extends DatagramChannel> onDatagramChannel(EventLoopGroup group) {
    return LazyHolder.getInstance().datagramChannel;
  }

  @Override
  public Class<? extends ServerChannel> onServerChannel(EventLoopGroup group) {
    return LazyHolder.getInstance().serverChannel;
  }

  private static class NetifiThreadFactory implements ThreadFactory {
    private AtomicInteger counter = new AtomicInteger();

    @Override
    public Thread newThread(Runnable r) {
      return new FastThreadLocalThread(r, "netifi-loop-" + counter.incrementAndGet());
    }
  }

  private static class LazyHolder {
    private static final LazyHolder INSTANCE = new LazyHolder();

    private static final NetifiThreadFactory FACTORY = new NetifiThreadFactory();

    private final EventLoopGroup eventLoopGroup;

    private final Class<? extends ServerChannel> serverChannel;

    private final Class<? extends Channel> channel;

    private final Class<? extends DatagramChannel> datagramChannel;

    private LazyHolder() {
      if (isEpollPresent()) {
        eventLoopGroup = new EpollEventLoopGroup(0, FACTORY);
        serverChannel = EpollServerSocketChannel.class;
        channel = EpollSocketChannel.class;
        datagramChannel = EpollDatagramChannel.class;
      } /*else if (isKQueuePresent()) {
          eventLoopGroup = new KQueueEventLoopGroup();
        }*/ else {
        eventLoopGroup = new NioEventLoopGroup(0, FACTORY);
        serverChannel = NioServerSocketChannel.class;
        channel = NioSocketChannel.class;
        datagramChannel = NioDatagramChannel.class;
      }
    }

    public static LazyHolder getInstance() {
      return INSTANCE;
    }

    boolean isEpollPresent() {
      try {
        Class.forName(
            "io.netty.channel.epoll.Epoll", false, Thread.currentThread().getContextClassLoader());
        return Epoll.isAvailable();
      } catch (ClassNotFoundException cnfe) {
        return false;
      }
    }
  }
}
