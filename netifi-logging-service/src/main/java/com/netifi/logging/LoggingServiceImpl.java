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
package com.netifi.logging;

import com.google.protobuf.StringValue;
import com.netifi.common.logging.StreamingLogAppender;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

/*
 * Streams Log4J 2 events to from a {@link StreamingLogAppender}. To use add packages to your
 * log4j2.xml like <Configuration packages="com.netifi" status="warn"> and then add an appender like
 * <StreamingLogAppender name="NetifiGatewayAppender" />. The service will find the appender by the
 * name given and stream the live log events.
 */
public class LoggingServiceImpl implements LoggingService {
  private final Scheduler scheduler;
  private final int maxWindowSize;
  private final int overflowBufferSize;
  private final ConcurrentHashMap<String, Flux<LogEvents>> logEventStreams;
  private final Duration maxWindowTime;

  public LoggingServiceImpl(Scheduler scheduler) {
    this(scheduler, 250, Duration.ofMillis(250), 1024);
  }

  public LoggingServiceImpl(
      Scheduler scheduler, int maxWindowSize, Duration maxWindowTime, int overflowBufferSize) {
    this.scheduler = scheduler;
    this.maxWindowSize = maxWindowSize;
    this.maxWindowTime = maxWindowTime;
    this.overflowBufferSize = overflowBufferSize;
    this.logEventStreams = new ConcurrentHashMap<>();
  }

  @Override
  public Flux<LogEvents> streamLogByAppenderName(StringValue message, ByteBuf metadata) {
    String appenderName = message.getValue();

    Map<String, Appender> appenders = getAppenders();
    Appender appender = appenders.get(appenderName);

    if (appender == null) {
      return Flux.error(new IllegalStateException("no appender found for name " + appenderName));
    }

    if (!(appender instanceof StreamingLogAppender)) {
      return Flux.error(
          new IllegalStateException(
              "the appender named "
                  + appenderName
                  + " is not an instance of "
                  + StreamingLogAppender.class.getName()));
    }

    StreamingLogAppender streamingLogAppender = (StreamingLogAppender) appender;

    return lookupLogEventStream(appenderName, streamingLogAppender)
        .onBackpressureBuffer(overflowBufferSize, BufferOverflowStrategy.DROP_OLDEST);
  }

  private Flux<LogEvents> lookupLogEventStream(
      String appenderName, StreamingLogAppender streamingLogAppender) {
    return logEventStreams.computeIfAbsent(
        appenderName,
        __ ->
            streamingLogAppender
                .streamLogs()
                .windowTimeout(maxWindowSize, maxWindowTime, scheduler)
                .concatMap(flux -> flux.reduce(LogEvents.newBuilder(), this::reduce))
                .map(LogEvents.Builder::build)
                .publish()
                .refCount()
                .doFinally(s -> logEventStreams.remove(appenderName)));
  }

  private LogEvents.Builder reduce(
      LogEvents.Builder builder, org.apache.logging.log4j.core.LogEvent logEvent) {
    com.netifi.logging.LogEvent.Builder logEventBuilder = com.netifi.logging.LogEvent.newBuilder();
    Message message = logEvent.getMessage();

    if (message instanceof StringBuilderFormattable) {
      StringBuilderFormattable formattable = (StringBuilderFormattable) message;
      StringBuilder sb = new StringBuilder();
      formattable.formatTo(sb);
      logEventBuilder.setMessage(sb.toString());
    } else {
      logEventBuilder.setMessage(message.getFormattedMessage());
    }

    String loggerName = logEvent.getLoggerName();

    logEventBuilder
        .setLoggerName(loggerName == null ? "NO_LOGGER_NAME" : loggerName)
        .setLevel(logEvent.getLevel().name());

    return builder.addEvents(logEventBuilder);
  }

  private Map<String, Appender> getAppenders() {
    Logger logger = LogManager.getLogger();
    return ((org.apache.logging.log4j.core.Logger) logger).getAppenders();
  }
}
