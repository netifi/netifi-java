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
package com.netifi.broker.auth.hashing;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

/** Implementations of this interface are used to hash access tokens. */
public interface AccessTokenHasher {
  static AccessTokenHasher instance(AccessTokenHashType type) {
    switch (type) {
      case PBKDF2WithHmacSHA1:
        return PBKDF2WithHmacSHA1AccessTokenHasher.INSTANCE;
      default:
        throw new IllegalArgumentException("unsupported hash type: " + type);
    }
  }

  static AccessTokenHasher defaultInstance() {
    return AccessTokenHasher.instance(AccessTokenHashType.PBKDF2WithHmacSHA1);
  }

  /**
   * Hashes a 160-bit access token
   *
   * @param salt long used to salt the hash
   * @param accessToken access token to hash
   * @return hashed access token
   */
  ByteBuf hash(ByteBuf salt, ByteBuf accessToken);

  default boolean verify(ByteBuf salt, ByteBuf accessToken, ByteBuf hash) {
    ByteBuf computed = hash(salt, accessToken);
    return ByteBufUtil.equals(hash, computed);
  }
}
