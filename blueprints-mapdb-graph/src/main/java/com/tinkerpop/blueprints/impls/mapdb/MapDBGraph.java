package com.tinkerpop.blueprints.impls.mapdb;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.VerticesFromEdgesIterable;
import org.mapdb.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * MapDB graph API
 */
public class MapDBGraph implements Graph {

    protected final DB db;
    protected final Engine engine;

    protected final Set<Long> vertices;
    protected final Set<Long> edges;

    protected final NavigableMap<Fun.Tuple2<Long,String>,Object> props;

    /** key:vertice recid, direction (out=true), edge label, edge recid*/
    protected final NavigableSet<Fun.Tuple4<Long,Boolean,String,Long>> edges4vertice;

    public class MElement implements  Element{

        protected final Long id;

        public MElement(Long id) {
            this.id = id;
        }

        @Override
        public <T> T getProperty(String key) {
            return (T) props.get(Fun.t2(id, key));
        }

        @Override
        public Set<String> getPropertyKeys() {
            Set<String> ret = new HashSet<String>();
            for(String s:Bind.findSecondaryKeys(props.navigableKeySet(),id)){
                ret.add(s);
            }
            return ret;
        }

        @Override
        public void setProperty(String key, Object value) {
            if(key==null||"".equals(key)||"id".equals(key)
                    ||"label".equals(key)) throw new IllegalArgumentException();
            props.put(Fun.t2(id,key),value);
        }

        @Override
        public <T> T removeProperty(String key) {
            return (T) props.remove(Fun.t2(id, key));
        }



        @Override
        public void remove() {
            ((NavigableMap)props).subMap(Fun.t2(id,null),Fun.t2(id,Fun.HI())).clear();
        }

        @Override
        public Object getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MElement)) return false;

            MElement mElement = (MElement) o;

            if (id != null ? !id.equals(mElement.id) : mElement.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }

    public class MVertex extends MElement implements Vertex{


        public MVertex(Long id) {
            super(id);
        }

        @Override
        public Iterable<Edge> getEdges(Direction direction, String... labels) {
            List<Edge> ret = new ArrayList<Edge>();
            if(labels==null || labels.length==0) labels = new String[]{null};
            for(String label:labels){

                if(Direction.BOTH == direction){
                    for(Long recid : Bind.findVals4( edges4vertice, id, true, label)){
                        ret.add(engine.get(recid,EDGE_SERIALIZER));
                    }
                    for(Long recid : Bind.findVals4( edges4vertice, id, false, label)){
                        ret.add(engine.get(recid,EDGE_SERIALIZER));
                    }
                }else{
                    for(Long recid : Bind.findVals4( edges4vertice, id, direction == Direction.OUT, label)){
                        ret.add(engine.get(recid,EDGE_SERIALIZER));
                    }
                }
            }
            return ret;
        }

        @Override
        public Iterable<Vertex> getVertices(final Direction direction, String... labels) {
            return new VerticesFromEdgesIterable(this,direction,labels);
        }

        @Override
        public VertexQuery query() {
            return new DefaultVertexQuery(this);
        }

        @Override
        public Edge addEdge(String label, Vertex inVertex) {
            return MapDBGraph.this.addEdge(null,this,inVertex,label);
        }



        @Override
        public void remove() {
            super.remove();
            engine.delete(id,VERTEX_SERIALIZER);
            vertices.remove(id);

            //remove related edges
            for(Edge e:getEdges(Direction.OUT))e.remove();
            for(Edge e:getEdges(Direction.IN))e.remove();

        }

    }

    protected final Serializer<MVertex> VERTEX_SERIALIZER = new Serializer<MVertex>() {
        @Override
        public void serialize(DataOutput out, MVertex value) throws IOException {
            if(value.id==null) return;
            Utils.packLong(out,value.id);
        }

        @Override
        public MVertex deserialize(DataInput in, int available) throws IOException {
            if(available==0) return VERTEX_EMPTY;
            return new MVertex(Utils.unpackLong(in));
        }
    };

    protected class MEdge extends MElement implements Edge{

        protected final long in,out;
        protected final String label;


        public MEdge(Long id, long out, long in,String label) {
            super(id);
            this.out = out;
            this.in = in;
            if(label==null && id!=null) throw new IllegalArgumentException();
            this.label = label;
        }


        @Override
        public Vertex getVertex(Direction direction) throws IllegalArgumentException {
            if (direction.equals(Direction.IN))
                return engine.get(in,VERTEX_SERIALIZER);
            else if (direction.equals(Direction.OUT))
                return engine.get(out,VERTEX_SERIALIZER);
            else
                throw ExceptionFactory.bothIsNotSupported();
        }

        @Override
        public String getLabel() {
            return label;
        }


        @Override
        public void remove() {
            super.remove();
            engine.delete(id,EDGE_SERIALIZER);
            edges.remove(id);
            edges4vertice.remove(Fun.t4(out,true,label,id));
            edges4vertice.remove(Fun.t4(in,false,label,id));
        }
    }

    protected final Serializer<MEdge> EDGE_SERIALIZER = new Serializer<MEdge>() {
        @Override
        public void serialize(DataOutput out, MEdge value) throws IOException {
            if(value.id==null) return;
            Utils.packLong(out,value.id);
            Utils.packLong(out,value.out);
            Utils.packLong(out,value.in);
            out.writeUTF(value.getLabel());
        }

        @Override
        public MEdge deserialize(DataInput in, int available) throws IOException {
            if(available==0) return EDGE_EMPTY;
            return new MEdge(Utils.unpackLong(in),Utils.unpackLong(in),Utils.unpackLong(in),in.readUTF());
        }
    };

    protected final MEdge EDGE_EMPTY = new MEdge(null,0L,0L,null);

    public MapDBGraph(String fileName) {
        File f = new File(fileName);
        f.getParentFile().mkdirs();
        db = DBMaker.newFileDB(f)
                .asyncWriteDisable()
                .transactionDisable()
                .make();
        engine = db.getEngine();

        vertices = db
                .createTreeSet("vertices")
                .makeLongSet();

        edges = db
                .createTreeSet("edges")
                .makeLongSet();

        edges4vertice = db
                .createTreeSet("edges4vertice")
                .serializer(BTreeKeySerializer.TUPLE4)
                .make();

        props = db.createTreeMap("props")
                .keySerializer(BTreeKeySerializer.TUPLE2)
                .valuesStoredOutsideNodes(true)
                .make();
    }

    protected final MVertex VERTEX_EMPTY = new MVertex(null);


    @Override
    public Vertex addVertex(Object id) {
        //preallocate recid
        Long recid = engine.put(VERTEX_EMPTY,VERTEX_SERIALIZER);
        //and insert real value
        MVertex v = new MVertex(recid);
        engine.update(recid, v, VERTEX_SERIALIZER);
        vertices.add(recid);
        return v;
    }

    @Override
    public Vertex getVertex(Object id) {
        if(id==null) throw new IllegalArgumentException();
        if(!vertices.contains(id))return null;
        return engine.get((Long)id, VERTEX_SERIALIZER);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return new Iterable<Vertex>() {

            Iterator<Long> vertIter = vertices.iterator();
            @Override
            public Iterator<Vertex> iterator() {
                return new Iterator<Vertex>() {
                    @Override
                    public boolean hasNext() {
                        return vertIter.hasNext();
                    }

                    @Override
                    public Vertex next() {
                        return engine.get(vertIter.next(),VERTEX_SERIALIZER);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        throw new UnsupportedOperationException(); //TODO filter props
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        Long recid = engine.put(EDGE_EMPTY, EDGE_SERIALIZER);
        MEdge edge = new MEdge(recid,(Long) outVertex.getId(), (Long) inVertex.getId(),label);
        edges.add(recid);
        engine.update(recid,edge,EDGE_SERIALIZER);
        edges4vertice.add(Fun.t4(edge.out,true,label,recid));
        edges4vertice.add(Fun.t4(edge.in,false,label,recid));
        return edge;
    }

    @Override
    public Edge getEdge(Object id) {
        if(id==null) throw new IllegalArgumentException();
        if(!edges.contains(id))return null;
        return engine.get((Long) id,EDGE_SERIALIZER);
    }

    @Override
    public void removeEdge(Edge edge) {
        edge.remove();
    }

    @Override
    public Iterable<Edge> getEdges() {
        return new Iterable<Edge>() {

            Iterator<Long> edgeIter = edges.iterator();
            @Override
            public Iterator<Edge> iterator() {
                return new Iterator<Edge>() {
                    @Override
                    public boolean hasNext() {
                        return edgeIter.hasNext();
                    }

                    @Override
                    public Edge next() {
                        return engine.get(edgeIter.next(),EDGE_SERIALIZER);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };

    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        throw new UnsupportedOperationException(); //TODO filter props
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    @Override
    public void shutdown() {
        db.close();
    }

    @Override
    public Features getFeatures() {
        Features f = new Features();
        f.supportsDuplicateEdges = true;
        f.supportsSelfLoops = true;
        f.supportsSerializableObjectProperty = true;
        f.supportsBooleanProperty = true;
        f.supportsDoubleProperty = true;
        f.supportsFloatProperty = true;
        f.supportsIntegerProperty = true;
        f.supportsPrimitiveArrayProperty = true;
        f.supportsUniformListProperty = true;
        f.supportsMixedListProperty = true;
        f.supportsLongProperty = true;
        f.supportsMapProperty = true;
        f.supportsStringProperty = true;

        f.ignoresSuppliedIds = true;
        f.isPersistent = true;
        f.isWrapper = false;

        f.supportsIndices = true;
        f.supportsKeyIndices = true;
        f.supportsVertexKeyIndex = true;
        f.supportsEdgeKeyIndex = true;
        f.supportsVertexIndex = true;
        f.supportsEdgeIndex = true;
        f.supportsTransactions = false;
        f.supportsVertexIteration = true;
        f.supportsEdgeIteration = true;
        f.supportsEdgeRetrieval = true;
        f.supportsVertexProperties = true;
        f.supportsEdgeProperties = true;
        f.supportsThreadedTransactions = false;
        return f;

    }

}
