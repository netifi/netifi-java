/*
 * Copyright 2002-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netifi.spring.messaging;

import com.netifi.broker.BrokerClient;
import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.reactive.HandlerMethodArgumentResolver;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.Assert;

import static com.netifi.spring.core.annotation.BrokerClientStaticFactory.resolveTags;
import static com.netifi.spring.messaging.RSocketRequesterStaticFactory.createRSocketRequester;
import static com.netifi.spring.messaging.RSocketRequesterStaticFactory.resolveBrokerClientRSocket;
import static org.springframework.core.annotation.AnnotatedElementUtils.getMergedAnnotation;

/**
 * Resolves arguments of type {@link RSocket} that can be used for making
 * requests to the remote peer.
 *
 * @author Rossen Stoyanchev
 * @since 5.2
 */
public class BrokerClientRequesterMethodArgumentResolver
    implements HandlerMethodArgumentResolver {

	private final String                     rSocketName;
	private final BrokerClient               brokerClient;
	private final DefaultListableBeanFactory listableBeanFactory;
	private final RSocketStrategies          rSocketStrategies;

	public BrokerClientRequesterMethodArgumentResolver(
		String rSocketName,
		BrokerClient client,
		DefaultListableBeanFactory factory,
		RSocketStrategies strategies
	) {
		this.rSocketName = rSocketName;
		brokerClient = client;
		listableBeanFactory = factory;
		rSocketStrategies = strategies;
	}

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		Class<?> type = parameter.getParameterType();
		return (RSocketRequester.class.equals(type) || RSocket.class.isAssignableFrom(type));
	}

	@Override
	public Mono<Object> resolveArgument(MethodParameter parameter, Message<?> message) {
		Class<?> type = parameter.getParameterType();
		com.netifi.spring.core.annotation.BrokerClient brokerClientAnnotation =
			getMergedAnnotation(parameter.getParameter(), com.netifi.spring.core.annotation.BrokerClient.class);

		Assert.notNull(
			brokerClientAnnotation,
			"Incorrect Method Parameter, make sure your parameter is annotated with the @BrokerClient annotation"
		);

		if (RSocketRequester.class.equals(type)) {
			return Mono.just(createRSocketRequester(
				rSocketName,
				brokerClient,
				brokerClientAnnotation,
				resolveTags(listableBeanFactory, brokerClientAnnotation),
				rSocketStrategies
			));
		}
		else if (RSocket.class.isAssignableFrom(type)) {
			return Mono.just(resolveBrokerClientRSocket(
				rSocketName,
				brokerClient,
				brokerClientAnnotation.type(),
				brokerClientAnnotation.group(),
				brokerClientAnnotation.destination(),
				resolveTags(listableBeanFactory, brokerClientAnnotation)
			));
		}
		else {
			return Mono.error(new IllegalArgumentException("Unexpected parameter type: " + parameter));
		}
	}

}
