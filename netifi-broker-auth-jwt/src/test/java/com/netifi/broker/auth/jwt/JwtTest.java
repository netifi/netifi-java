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
package com.netifi.broker.auth.jwt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashSet;
import org.junit.Assert;
import org.junit.Test;

public class JwtTest {
  private static String jwt =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhbGlJZCI6IjExMTE4ODg4OCIsInN1YiI6InRlc3RpbmctYXBwIiwiYXVkIjoiamFja3ku"
          + "Y2hlbmxiQGFsaWJhYmEtaW5jLmNvbSIsInNhcyI6WyJkZWZhdWx0IiwiYWNjb3VudCIsInRyYW5zYWN0aW9uIl0sInJvbGVzIjpbImludGV"
          + "ybmFsIl0sImlzcyI6IlJTb2NrZXRCcm9rZXIiLCJvcmdzIjpbImFsaWJhYmEiLCJhbGlwYXkiXSwiaWF0IjoxNTY1NjMzMzA5fQ.bnQnr3F"
          + "22X_LT1V8dpdDHFIIuh3KZGEEIFoY6d6TOopw6bjx197g5nS9CscW3vsWvmwVj0R-wURUg92csKh06qyRdVQ9kn8IuD_6duVnxsGF22i8K"
          + "qunTlMuotLMe-k6-o16mYZcNiTyQYuWf1biySYB1TF-0n9t-w4gMGFADd8MlE18R9J0gjY0Nb3tWoK-sjVtwransviE9-lJzGop2kYPBW_"
          + "GtHX86M1nUPHQzFSOY2EkB6oUpZK3pbJhi_FxwFNm6BwnObKkxo3xT-9FM2s9lW71-sPFCQ-E7hivzF0viHQn0kk-5xC80iwP0qcvJQ-2"
          + "YlH84dANHPTzJiGhHQ";

  private static ByteBuf JWT;

  static {
    JWT = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, jwt);
  }

  @Test
  public void testHeader() {
    ByteBuf header = Jwt.header(JWT);
    String s = header.toString(StandardCharsets.UTF_8);
    Assert.assertEquals("eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9", s);
  }

  @Test
  public void TestPayload() {
    ByteBuf header = Jwt.payload(JWT);
    String s = header.toString(StandardCharsets.UTF_8);
    Assert.assertEquals(
        "eyJhbGlJZCI6IjExMTE4ODg4OCIsInN1YiI6InRlc3RpbmctYXBwIiwiYXVkIjoiamFja3kuY2hlbmxi"
            + "QGFsaWJhYmEtaW5jLmNvbSIsInNhcyI6WyJkZWZhdWx0IiwiYWNjb3VudCIsInRyYW5zYWN0aW9uIl0sInJvbGV"
            + "zIjpbImludGVybmFsIl0sImlzcyI6IlJTb2NrZXRCcm9rZXIiLCJvcmdzIjpbImFsaWJhYmEiLCJhbGlwYXkiXS"
            + "wiaWF0IjoxNTY1NjMzMzA5fQ",
        s);
  }

  @Test
  public void testSignature() {
    ByteBuf header = Jwt.signature(JWT);
    String s = header.toString(StandardCharsets.UTF_8);
    Assert.assertEquals(
        "bnQnr3F22X_LT1V8dpdDHFIIuh3KZGEEIFoY6d6TOopw6bjx197g5nS9CscW3vsWvmwVj0R-wURUg92csKh06qyRdVQ9kn8IuD_6duVnxsGF22i8KqunTlMuotLMe-k6-o16mYZcNiTyQYuWf1biySYB1TF-0n9t-w4gMGFADd8MlE18R9J0gjY0Nb3tWoK-sjVtwransviE9-lJzGop2kYPBW_GtHX86M1nUPHQzFSOY2EkB6oUpZK3pbJhi_FxwFNm6BwnObKkxo3xT-9FM2s9lW71-sPFCQ-E7hivzF0viHQn0kk-5xC80iwP0qcvJQ-2YlH84dANHPTzJiGhHQ",
        s);
  }

  @Test
  public void testHeaderAndPayload() {
    ByteBuf header = Jwt.headerAndPayload(JWT);
    String s = header.toString(StandardCharsets.UTF_8);
    Assert.assertEquals(
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhbGlJZCI6IjExMTE4ODg4OCIsInN1YiI6InRlc"
            + "3RpbmctYXBwIiwiYXVkIjoiamFja3kuY2hlbmxiQGFsaWJhYmEtaW5jLmNvbSIsInNhcyI6WyJkZWZhdWx0Iiw"
            + "iYWNjb3VudCIsInRyYW5zYWN0aW9uIl0sInJvbGVzIjpbImludGVybmFsIl0sImlzcyI6IlJTb2NrZXRCcm9r"
            + "ZXIiLCJvcmdzIjpbImFsaWJhYmEiLCJhbGlwYXkiXSwiaWF0IjoxNTY1NjMzMzA5fQ",
        s);
  }

  @Test
  public void testVerifySHA256withRSA() throws Exception {
    InputStream is =
        ClassLoader.getSystemClassLoader().getResourceAsStream("rsocket_broker_public_key.der");

    ByteBuf buf = Unpooled.buffer();
    int b;
    while ((b = is.read()) != -1) {
      buf.writeByte(b);
    }

    byte[] keyBytes = new byte[buf.readableBytes()];
    buf.readBytes(keyBytes);
    X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
    RSAPublicKey publicKey = (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(spec);

    boolean valid = Jwt.verifySHA256withRSA(publicKey, JWT);
    Assert.assertTrue(valid);
  }

  @Test
  public void testParseAccessKey() throws Exception {
    HashSet<String> set = new HashSet<>();
    set.add("aliId");
    long l = Jwt.parseAccessKey(JWT, set);
    Assert.assertEquals(111188888, l);
  }

  @Test
  public void testParseAccessKeyNotFound() throws Exception {
    HashSet<String> set = new HashSet<>();
    set.add("notFound");
    long l = Jwt.parseAccessKey(JWT, set);
    Assert.assertEquals(-1, l);
  }

  @Test
  public void testParseAlg() {
    String s = Jwt.parseAlgorithm(JWT);
    Assert.assertEquals("RS256", s);
  }

  @Test
  public void testParseAlgAndVerify() throws Exception {

    InputStream is =
        ClassLoader.getSystemClassLoader().getResourceAsStream("rsocket_broker_public_key.der");

    ByteBuf buf = Unpooled.buffer();
    int b;
    while ((b = is.read()) != -1) {
      buf.writeByte(b);
    }

    byte[] keyBytes = new byte[buf.readableBytes()];
    buf.readBytes(keyBytes);
    X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
    RSAPublicKey publicKey = (RSAPublicKey) KeyFactory.getInstance("RSA").generatePublic(spec);

    String s = Jwt.parseAlgorithm(JWT);
    boolean verify = Jwt.verify(s, publicKey, JWT);
    Assert.assertTrue(verify);
  }
}
