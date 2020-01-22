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
package com.netifi.broker.auth;

import com.netifi.common.time.Clock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/** */
public class DefaultSessionUtilTest {
  TestClock clock = new TestClock();
  DefaultSessionUtil sessionUtil = new DefaultSessionUtil(clock);

  @Test
  public void testGetSteps() {
    clock.setTime(0);
    long thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(0, thirtySecondsStepsFromEpoch);
    clock.setTime(30000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(1, thirtySecondsStepsFromEpoch);
    clock.setTime(35000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(1, thirtySecondsStepsFromEpoch);
    clock.setTime(60000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(2, thirtySecondsStepsFromEpoch);
    clock.setTime(63000);
    thirtySecondsStepsFromEpoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    Assert.assertEquals(2, thirtySecondsStepsFromEpoch);
  }

  @Test
  public void testGenerateToken() {
    clock.setTime(0);

    ByteBuf key = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "super secret password");
    ByteBuf message = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "hello world!");

    long epoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    ByteBuf m1 = sessionUtil.generateSessionToken(key, Unpooled.wrappedBuffer(message), epoch);

    ByteBuf m3 = sessionUtil.generateSessionToken(key, Unpooled.wrappedBuffer(message), epoch + 1);

    Assert.assertNotNull(m1);
    Assert.assertNotNull(m3);

    Assert.assertFalse(ByteBufUtil.equals(m1, m3));

    m1.release();
    m3.release();

    for (int i = 0; i < 100_000; i++) {
      sessionUtil.generateSessionToken(key, Unpooled.wrappedBuffer(message), epoch).release();
    }
  }

  @Test
  public void testGenerateRequestToken() {
    clock.setTime(0);
    ByteBuf key = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "super secret password");
    String destination = "test";
    long epoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    ByteBuf sessionToken =
        sessionUtil.generateSessionToken(
            key, Unpooled.wrappedBuffer(destination.getBytes()), epoch);

    int r1 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("a new request".getBytes()), epoch);

    clock.setTime(40000);
    int r2 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("a new request".getBytes()), epoch);
    Assert.assertEquals(r1, r2);
    clock.setTime(40000);

    int r3 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("another request".getBytes()), epoch + 1);
    Assert.assertNotEquals(r1, r3);

    clock.setTime(60000);
    int r4 =
        sessionUtil.generateRequestToken(
            sessionToken, Unpooled.wrappedBuffer("a new request".getBytes()), epoch + 2);
    Assert.assertNotEquals(r1, r4);
  }

  @Test
  public void testValidateMessage() {
    clock.setTime(0);
    ByteBuf key = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "super secret password");
    String destination = "test";
    long epoch = sessionUtil.getThirtySecondsStepsFromEpoch();
    ByteBuf sessionToken =
        sessionUtil.generateSessionToken(
            key, Unpooled.wrappedBuffer(destination.getBytes()), epoch);
    byte[] message = "a request".getBytes();

    int requestToken =
        sessionUtil.generateRequestToken(sessionToken, Unpooled.wrappedBuffer(message), epoch + 1);

    boolean valid =
        sessionUtil.validateMessage(
            sessionToken, Unpooled.wrappedBuffer(message), requestToken, epoch + 1);
    Assert.assertTrue(valid);

    clock.setTime(40000);
    valid =
        sessionUtil.validateMessage(
            sessionToken, Unpooled.wrappedBuffer(message), requestToken, epoch + 2);
    Assert.assertFalse(valid);
  }

  class TestClock implements Clock {
    long time;

    public long getTime() {
      return time;
    }

    public void setTime(long time) {
      this.time = time;
    }

    @Override
    public long getEpochTime() {
      return time;
    }
  }
}
