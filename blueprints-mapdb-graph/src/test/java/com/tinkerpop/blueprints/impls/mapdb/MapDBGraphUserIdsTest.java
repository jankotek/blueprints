package com.tinkerpop.blueprints.impls.mapdb;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class MapDBGraphUserIdsTest extends MapDBGraphTest {

    @Override
    public Graph generateGraph(final String graphDirectoryName) {
        return new MapDBGraph(getDirectory() + "/" + graphDirectoryName, true);
    }

    @Test
    public void testMVertexHashCodeAndEquals() {
        // Setup
        Graph graph = generateGraph();
        Random random = new Random();
        Map<Vertex, Edge> map = new HashMap<Vertex, Edge>();
        Vertex v1 = null, v2 = graph.addVertex(random.nextLong());
        Edge e = null;

        // Action
        for (int i = 0; i <= 100000; i++) {
            v1 = v2;
            v2 = graph.addVertex(random.nextLong());
            e = graph.addEdge(UUID.randomUUID().toString(), v1, v2, "likes");
            map.put(v2, e);
        }

        // Assert
        for (int i = 1; i <= 100000; i++) {
            v1 = e.getVertex(Direction.OUT);
            assertNotNull("Failed after " + i + " loops", e);
            e = map.get(v1);
        }
    }

    @Test
    public void testMEdgeHashCodeAndEquals() {
        // Setup
        Graph graph = generateGraph();
        Random random = new Random();
        Map<Edge, Vertex> map = new HashMap<Edge, Vertex>();
        Vertex v1 = null, v2 = graph.addVertex(random.nextLong());
        Edge e = null;

        // Action
        for (int i = 0; i <= 100000; i++) {
            v1 = v2;
            v2 = graph.addVertex(random.nextLong());
            e = graph.addEdge(UUID.randomUUID().toString(), v1, v2, "likes");
            map.put(e, v1);
        }

        // Assert
        for (int i = 1; i <= 100000; i++) {
            v1 = map.get(e);
            assertNotNull("Failed after " + i + " loops", v1);
            e = v1.getEdges(Direction.IN).iterator().next();
        }
    }

}
