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

import com.netifi.common.tags.Tag;
import com.netifi.common.tags.Tags;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore // can't reload system properties
public class DefaultBuilderConfigTest {

  @Test
  public void testShouldFindSingleSeedAddress() {
    System.setProperty("netifi.client.seedAddresses", "localhost:8001");
    List<InetSocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
    Assert.assertNotNull(seedAddress);
    Assert.assertEquals(1, seedAddress.size());

    InetSocketAddress address = (InetSocketAddress) seedAddress.get(0);
    Assert.assertEquals(8001, address.getPort());
  }

  @Test
  public void testShouldFindMultipleSeedAddresses() {
    System.setProperty(
        "netifi.client.seedAddresses", "localhost:8001,localhost:8002,localhost:8003");
    List<InetSocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
    Assert.assertNotNull(seedAddress);
    Assert.assertEquals(3, seedAddress.size());

    InetSocketAddress address = (InetSocketAddress) seedAddress.get(0);
    Assert.assertEquals(8001, address.getPort());
  }

  @Test(expected = IllegalStateException.class)
  public void testShouldThrowExceptionForAddressMissingPort() {
    System.setProperty("netifi.client.seedAddresses", "localhost:8001,localhost,localhost:8003");
    List<InetSocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
  }

  @Test(expected = IllegalStateException.class)
  public void testShouldThrowExceptionForInvalidAddress() {
    System.setProperty("netifi.client.seedAddresses", "no way im valid");
    List<InetSocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();
  }

  @Test
  public void testShouldParseTagsSuccessfully() {
    System.setProperty("netifi.client.tags.com.netifi.destination", "myDestination");
    Tags tags = DefaultBuilderConfig.getTags();
    Optional<Tag> first = tags.stream().findFirst();

    Assert.assertTrue(first.isPresent());
    Assert.assertEquals(first.get(), Tag.of("com.netifi.destination", "myDestination"));
  }

  @Test
  public void testShouldReturnNull() {
    List<InetSocketAddress> seedAddress = DefaultBuilderConfig.getSeedAddress();

    Assert.assertNull(seedAddress);
  }
}
