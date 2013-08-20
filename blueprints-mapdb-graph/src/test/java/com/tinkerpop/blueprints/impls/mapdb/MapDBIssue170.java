package com.tinkerpop.blueprints.impls.mapdb;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DBMaker;
import org.mapdb.Utils;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertFalse;

public class MapDBIssue170 {

    private Graph graph;

    @Before
    public void setUp() {
        File dbFile = Utils.tempDbFile();
        DBMaker dbMaker = DBMaker.newFileDB(dbFile)
                .asyncWriteDisable()
                .transactionDisable()
                .compressionEnable() // This causes troubles
                .deleteFilesAfterClose()
                .closeOnJvmShutdown();
        graph = new MapDBGraph(dbMaker, true);
    }

    @Test
    public void testArrayStoreException() {
        // Action
        boolean exceptionThrown = false;
        try {
            long i = 0;
            Vertex tail, head;
            while (i < 10E5) {
                tail = addOrGetVertex(i);
                head = addOrGetVertex(i++);
                Edge edge = graph.addEdge(UUID.randomUUID().toString(), tail, head, "precedes");
                edge.setProperty("myId", new Long(i));
            }
        } catch (ArrayStoreException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }

        // Assert
        assertFalse("ArrayStoreException was thrown", exceptionThrown);
    }

    public Vertex addOrGetVertex(Object id) {
        Vertex v = graph.getVertex(id);
        if (v != null) return v;
        v = graph.addVertex(id);
        v.setProperty("myId", id);
        return v;
    }

}