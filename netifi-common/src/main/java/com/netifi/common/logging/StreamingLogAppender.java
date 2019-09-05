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
package com.netifi.common.logging;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

@Plugin(
    name = "StreamingLogAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE)
public class StreamingLogAppender extends AbstractAppender {

  private static final DirectProcessor<String> logEvents = DirectProcessor.create();

  protected StreamingLogAppender(String name, Filter filter) {
    super(name, filter, null);
  }

  @PluginFactory
  public static StreamingLogAppender createAppender(
      @PluginAttribute("name") String name, @PluginElement("Filter") Filter filter) {
    return new StreamingLogAppender(name, filter);
  }

  @Override
  public void append(LogEvent event) {
    logEvents.onNext(event.getMessage().getFormattedMessage());
  }

  public Flux<String> streamLogs() {
    return logEvents.onBackpressureDrop();
  }
}
