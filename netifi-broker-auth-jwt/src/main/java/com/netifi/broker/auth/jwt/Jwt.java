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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;
import java.util.Set;

public class Jwt {
  private static final byte JWT_SEPARATOR = (byte) '.';

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();

  public static ByteBuf header(ByteBuf token) {

    int length =
        ByteBufUtil.indexOf(token, token.readerIndex(), token.readableBytes(), JWT_SEPARATOR);

    return token.slice(token.readerIndex(), length);
  }

  public static ByteBuf payload(ByteBuf token) {
    int index =
        ByteBufUtil.indexOf(token, token.readerIndex(), token.readableBytes(), JWT_SEPARATOR);

    int length = ByteBufUtil.indexOf(token, index + 1, token.readableBytes(), JWT_SEPARATOR);

    return token.slice(index + 1, length - index - 1);
  }

  public static ByteBuf signature(ByteBuf token) {
    int index =
        ByteBufUtil.indexOf(token, token.readerIndex(), token.readableBytes(), JWT_SEPARATOR);

    index = ByteBufUtil.indexOf(token, index + 1, token.readableBytes(), JWT_SEPARATOR);

    return token.slice(index + 1, token.readableBytes() - index - 1);
  }

  public static ByteBuf headerAndPayload(ByteBuf token) {
    int index =
        ByteBufUtil.indexOf(token, token.readerIndex(), token.readableBytes(), JWT_SEPARATOR);

    int length = ByteBufUtil.indexOf(token, index + 1, token.readableBytes(), JWT_SEPARATOR);

    return token.slice(token.readerIndex(), length);
  }

  public static boolean verifySHA256withRSA(PublicKey publicKey, ByteBuf token) {
    return doVerify("SHA256withRSA", publicKey, token);
  }

  public static boolean verify(String alg, PublicKey publicKey, ByteBuf token) {
    switch (alg) {
      case "RS256":
        return verifySHA256withRSA(publicKey, token);
      default:
        throw new IllegalArgumentException("unsupported algorithm " + alg);
    }
  }

  private static boolean doVerify(String algorithm, PublicKey publicKey, ByteBuf token) {
    try {
      Signature signature = Signature.getInstance(algorithm);
      return doVerify(signature, publicKey, token);
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
  }

  private static boolean doVerify(Signature signature, PublicKey publicKey, ByteBuf token) {
    boolean verify;
    try {

      ByteBuf s = signature(token);
      ByteBuffer b = URL_DECODER.decode(s.nioBuffer());

      signature.initVerify(publicKey);
      ByteBuffer data = headerAndPayload(token).nioBuffer();
      signature.update(data);
      verify = signature.verify(b.array());

    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
    return verify;
  }

  public static long parseAccessKey(ByteBuf token, Set<String> mappedFields) {
    try {
      ByteBuf payload = payload(token);

      ByteBuffer decodedPayload = URL_DECODER.decode(payload.nioBuffer());

      InputStreamWrapper is = IS_WRAPPER.get();
      is.wrap(decodedPayload);
      JsonParser parser = JSON_FACTORY.createParser(is);

      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String name = parser.getCurrentName();
        if (mappedFields.contains(name)) {
          parser.nextToken();
          return Long.valueOf(parser.getText());
        }
      }

    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
    return -1;
  }

  public static String parseAlgorithm(ByteBuf token) {
    try {
      ByteBuf header = header(token);
      ByteBuffer decodedHeader = URL_DECODER.decode(header.nioBuffer());

      InputStreamWrapper is = IS_WRAPPER.get();
      is.wrap(decodedHeader);
      JsonParser parser = JSON_FACTORY.createParser(is);

      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String name = parser.getCurrentName();
        if ("alg".equals(name)) {
          parser.nextToken();
          return parser.getText();
        }
      }
    } catch (Exception e) {
      throw Exceptions.propagate(e);
    }
    return "unknown";
  }

  private static final FastThreadLocal<InputStreamWrapper> IS_WRAPPER =
      new FastThreadLocal<InputStreamWrapper>() {
        protected InputStreamWrapper initialValue() throws Exception {
          return new InputStreamWrapper();
        }
      };

  private static class InputStreamWrapper extends InputStream {

    ByteBuffer buf;

    public void wrap(ByteBuffer buf) {
      this.buf = buf;
    }

    public int read() throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }
      return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
      if (!buf.hasRemaining()) {
        return -1;
      }

      len = Math.min(len, buf.remaining());
      buf.get(bytes, off, len);
      return len;
    }
  }
}
