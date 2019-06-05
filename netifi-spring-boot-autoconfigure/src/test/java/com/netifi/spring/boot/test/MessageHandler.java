package com.netifi.spring.boot.test;

import reactor.core.publisher.Mono;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

@Controller
@MessageMapping("test")
public class MessageHandler {

    @MessageMapping("process")
    public Mono<String> process(@Payload Mono<String> data) {
        return Mono.just("Echo: " + data);
    }
}
