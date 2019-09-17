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
package com.netifi.broker.auth;

import com.netifi.common.time.Clock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** */
class DefaultSessionUtil extends SessionUtil {
  private static final String ALGORITHM = "HmacSHA1";
  private final Clock clock;

  DefaultSessionUtil(Clock clock) {
    this.clock = clock;
  }

  DefaultSessionUtil() {
    this(Clock.DEFAULT);
  }

  @Override
  public ByteBuf generateSessionToken(ByteBuf key, ByteBuf data, long count) {
    key.resetReaderIndex();
    data.resetReaderIndex();

    ByteBuf buffer = ByteBufAllocator.DEFAULT.heapBuffer(key.readableBytes() + 8);

    try {
      buffer.writeBytes(key).writeLong(count);
      SecretKeySpec keySpec =
          new SecretKeySpec(buffer.array(), 0, buffer.readableBytes(), ALGORITHM);
      Mac mac = Mac.getInstance(ALGORITHM);
      mac.init(keySpec);
      mac.update(data.nioBuffer());
      return Unpooled.wrappedBuffer(mac.doFinal());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      buffer.release();
    }
  }

  @Override
  public int generateRequestToken(ByteBuf sessionToken, ByteBuf message, long count) {
    ByteBuf bytes = generateSessionToken(sessionToken, message, count);
    return bytes.getInt(bytes.readerIndex());
  }

  @Override
  public boolean validateMessage(
      ByteBuf sessionToken, ByteBuf message, int requestToken, long count) {
    int generatedToken = generateRequestToken(sessionToken, message, count);
    return requestToken == generatedToken;
  }

  public long getThirtySecondsStepsFromEpoch() {
    return clock.getEpochTime() / 30000;
  }
}
