package com.netifi.broker.rsocket.ipc.routing;

import java.util.HashMap;
import java.util.Map;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.ipc.MutableRouter;
import io.rsocket.ipc.util.IPCChannelFunction;
import io.rsocket.ipc.util.IPCFunction;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ForwardingRouter implements MutableRouter<ForwardingRouter> {


	private final Map<String, IPCFunction<Mono<Void>>>    fireAndForgetRegistry;
	private final   Map<String, IPCFunction<Mono<Payload>>> requestResponseRegistry;
	private final   Map<String, IPCFunction<Flux<Payload>>> requestStreamRegistry;
	private final   Map<String, IPCChannelFunction>         requestChannelRegistry;
	private final IPCFunction<Mono<Void>>    fnfForward;
	private final IPCFunction<Mono<Payload>> requestResponseForward;
	private final IPCFunction<Flux<Payload>> requestStreamForward;
	private final IPCChannelFunction         requestChannelForward;

	public ForwardingRouter(RSocket rSocketToForward) {
		this.fireAndForgetRegistry = new HashMap<>();
		this.requestResponseRegistry = new HashMap<>();
		this.requestStreamRegistry = new HashMap<>();
		this.requestChannelRegistry = new HashMap<>();

		this.fnfForward = (payload, metadata) -> rSocketToForward.fireAndForget(payload);
		this.requestResponseForward = (payload, metadata) -> rSocketToForward.requestResponse(payload);
		this.requestStreamForward = (payload, metadata) -> rSocketToForward.requestStream(payload);
		this.requestChannelForward = (payloads, firstPayload, metadata) -> rSocketToForward.requestChannel(payloads);
	}

	@Override
	public IPCFunction<Mono<Void>> routeFireAndForget(String route) {
		return fireAndForgetRegistry.getOrDefault(route, fnfForward);
	}

	@Override
	public IPCFunction<Mono<Payload>> routeRequestResponse(String route) {
		return requestResponseRegistry.getOrDefault(route, requestResponseForward);
	}

	@Override
	public IPCFunction<Flux<Payload>> routeRequestStream(String route) {
		return requestStreamRegistry.getOrDefault(route, requestStreamForward);
	}

	@Override
	public IPCChannelFunction routeRequestChannel(String route) {
		return requestChannelRegistry.getOrDefault(route, requestChannelForward);
	}

	@Override
	public ForwardingRouter withFireAndForgetRoute(String route, IPCFunction<Mono<Void>> function) {
		fireAndForgetRegistry.put(route, function);
		return this;
	}

	@Override
	public ForwardingRouter withRequestResponseRoute(String route, IPCFunction<Mono<Payload>> function) {
		requestResponseRegistry.put(route, function);
		return this;
	}

	@Override
	public ForwardingRouter withRequestStreamRoute(String route, IPCFunction<Flux<Payload>> function) {
		requestStreamRegistry.put(route, function);
		return this;
	}

	@Override
	public ForwardingRouter withRequestChannelRoute(String route, IPCChannelFunction function) {
		requestChannelRegistry.put(route, function);
		return this;
	}

	public Map<String, IPCFunction<Mono<Void>>> getFireAndForgetRegistry() {
		return fireAndForgetRegistry;
	}

	public Map<String, IPCFunction<Mono<Payload>>> getRequestResponseRegistry() {
		return requestResponseRegistry;
	}

	public Map<String, IPCFunction<Flux<Payload>>> getRequestStreamRegistry() {
		return requestStreamRegistry;
	}

	public Map<String, IPCChannelFunction> getRequestChannelRegistry() {
		return requestChannelRegistry;
	}
}
