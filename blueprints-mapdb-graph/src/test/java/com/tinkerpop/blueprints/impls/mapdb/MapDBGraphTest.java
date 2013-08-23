package com.tinkerpop.blueprints.impls.mapdb;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;

import java.io.File;
import java.lang.reflect.Method;
import java.util.UUID;

public class MapDBGraphTest extends GraphTest {

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testKeyIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new KeyIndexableGraphTestSuite(this));
        printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexableGraphTestSuite(this));
        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

    public void testVertexQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexQueryTestSuite(this));
        printTestPerformance("VertexQueryTestSuite", this.stopWatch());
    }

    public void testGraphQueryTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphQueryTestSuite(this));
        printTestPerformance("GraphQueryTestSuite", this.stopWatch());
    }

    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    public void testGraphSONReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphSONReaderTestSuite(this));
        printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
    }

    public void testShutdownStartManyTimes() {
        // Setup
        Graph graph = generateGraph();

        // Action
        for (int i = 0; i < 25; i++) {
            Vertex a = graph.addVertex(null);
            a.setProperty("name", "a" + UUID.randomUUID());
            Vertex b = graph.addVertex(null);
            b.setProperty("name", "b" + UUID.randomUUID());
            graph.addEdge(null, a, b, "knows").setProperty("weight", 1);
        }
        graph.shutdown();
        this.stopWatch();

        // Assert
        int iterations = 150;
        for (int i = 0; i < iterations; i++) {
            graph = (MapDBGraph) generateGraph();
            assertEquals(50, count(graph.getVertices()));
            for (final Vertex v : graph.getVertices()) {
                assertTrue(v.getProperty("name").toString().startsWith("a")
                        || v.getProperty("name").toString().startsWith("b"));
            }
            assertEquals(25, count(graph.getEdges()));
            for (final Edge e : graph.getEdges()) {
                assertEquals(e.getProperty("weight"), 1);
            }
            graph.shutdown();
        }
        printPerformance(graph.toString(), iterations, "iterations of shutdown and restart", this.stopWatch());
    }

    @Override
    public Graph generateGraph() {
        return generateGraph("graph-test.db");
    }

    @Override
    public Graph generateGraph(String name) {
        return new MapDBGraph(getDirectory() + "/" + name, false);
    }

    protected String getDirectory() {
        return System.getProperty("graphDirectory", computeTestDataRoot().getAbsolutePath());
    }

    @Override
    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String directory = getDirectory();
        deleteDirectory(new File(directory));
        for (Method method : testSuite.getClass().getDeclaredMethods()) {
            if (method.getName().startsWith("test")) {
                System.out.println("Testing " + method.getName() + "...");
                method.invoke(testSuite);
                deleteDirectory(new File(directory));
            }
        }
    }

}
