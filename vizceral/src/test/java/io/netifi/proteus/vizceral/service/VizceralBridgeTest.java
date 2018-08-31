package io.netifi.proteus.vizceral.service;

import com.google.protobuf.Empty;
import io.netifi.proteus.tracing.TracesStreamer;
import io.netifi.proteus.viz.Connection;
import io.netifi.proteus.viz.Metrics;
import io.netifi.proteus.viz.Node;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VizceralBridgeTest {

  private TracesStreamer tracesStreamer;

  @Before
  public void setUp() {
    tracesStreamer = new TracesStreamer(zipkinSource());
  }

  @Test
  public void vizSource() {
    VizceralBridge vizceralBridge = new VizceralBridge(tracesStreamer.streamTraces());
    Node root = vizceralBridge
        .visualisations(Empty.getDefaultInstance(), Unpooled.EMPTY_BUFFER)
        .blockFirst(Duration.ofSeconds(5));

    assertNotNull(root);

    List<Connection> connectionsList = root.getConnectionsList();
    assertNotNull(connectionsList);
    assertEquals(1, connectionsList.size());

    Connection conn = connectionsList.iterator().next();
    assertEquals(
        "quickstart.clients-client1-io.netifi.proteus.quickstart.service.HelloService",
        conn.getSource());
    assertEquals(
        "quickstart.services.helloservices-helloservice-3fa7b9dc-7afd-4767-a781-b7265a9fa02d-io.netifi.proteus.quickstart.service.HelloService",
        conn.getTarget());

    Metrics metrics = conn.getMetrics();
    assertEquals(1.0d, metrics.getNormal(), 1e-7);
    assertEquals(0.0d, metrics.getDanger(), 1e-7);

    List<Node> nodeList = root.getNodesList();
    assertNotNull(nodeList);
    assertEquals(2, nodeList.size());
  }

  private Mono<InputStream> zipkinSource() {
    return Mono.fromCallable(() ->
        getClass().getClassLoader().getResourceAsStream("zipkin.json"));
  }
}
