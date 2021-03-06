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
package com.netifi.broker.frames;

import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GroupFlyweight {
  public static ByteBuf encode(
      ByteBufAllocator allocator, CharSequence group, ByteBuf metadata, Tags tags) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeFrameHeader(allocator, FrameType.GROUP);

    int groupLength = ByteBufUtil.utf8Bytes(group);
    byteBuf.writeInt(groupLength);
    ByteBufUtil.reserveAndWriteUtf8(byteBuf, group, groupLength);

    int metadataLength = metadata.readableBytes();
    byteBuf.writeInt(metadataLength).writeBytes(metadata, metadata.readerIndex(), metadataLength);

    for (Tag tag : tags) {
      String key = tag.getKey();
      String value = tag.getValue();

      int keyLength = ByteBufUtil.utf8Bytes(key);
      byteBuf.writeInt(keyLength);
      ByteBufUtil.reserveAndWriteUtf8(byteBuf, key, keyLength);

      int valueLength = ByteBufUtil.utf8Bytes(value);
      byteBuf.writeInt(valueLength);
      ByteBufUtil.reserveAndWriteUtf8(byteBuf, value, valueLength);
    }

    return byteBuf;
  }

  public static String group(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.toString(offset, groupLength, StandardCharsets.UTF_8);
  }

  public static ByteBuf metadata(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength;

    int metadataLength = byteBuf.getInt(offset);
    offset += Integer.BYTES;

    return byteBuf.slice(offset, metadataLength);
  }

  public static Tags tags(ByteBuf byteBuf) {
    int offset = FrameHeaderFlyweight.BYTES;

    int groupLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + groupLength;

    int metadataLength = byteBuf.getInt(offset);
    offset += Integer.BYTES + metadataLength;

    List<Tag> tags = new ArrayList<>();
    while (offset < byteBuf.readableBytes()) {
      int keyLength = byteBuf.getInt(offset);
      offset += Integer.BYTES;

      String key = byteBuf.toString(offset, keyLength, StandardCharsets.UTF_8);
      offset += keyLength;

      int valueLength = byteBuf.getInt(offset);
      offset += Integer.BYTES;

      String value = byteBuf.toString(offset, valueLength, StandardCharsets.UTF_8);
      offset += valueLength;

      tags.add(Tag.of(key, value));
    }

    return Tags.of(tags);
  }
}
