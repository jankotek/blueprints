package com.tinkerpop.blueprints.impls.mapdb;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.*;
import org.mapdb.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * MapDB graph API
 */
public class MapDBGraph implements IndexableGraph,KeyIndexableGraph {

    protected final DB db;
    protected final Engine engine;

    protected final Set<Long> vertices;
    protected final Set<Long> edges;

    protected final NavigableMap<Fun.Tuple2<Long,String>,Object> verticesProps;
    protected final NavigableMap<Fun.Tuple2<Long,String>,Object> edgesProps;

    protected final NavigableSet<Fun.Tuple3<String,Object,Long>> verticesIndex;
    protected final NavigableSet<Fun.Tuple3<String,Object,Long>> edgesIndex;

    protected final NavigableSet<Fun.Tuple4<String,String,Object,Long>> verticesIndex2;
    protected final NavigableSet<Fun.Tuple4<String,String,Object,Long>> edgesIndex2;

    protected final Set<String> verticesKeys;
    protected final Set<String> edgesKeys;

    protected final Set<String> verticesKeys2;
    protected final Set<String> edgesKeys2;


    /** key:vertice recid, direction (out=true), edge label, edge recid*/
    protected final NavigableSet<Fun.Tuple4<Long,Boolean,String,Long>> edges4vertice;

    protected final File directory;


    public class MVertex implements Vertex{


        protected final Long id;

        public MVertex(Long id) {
            this.id = id;
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
            if(!vertices.contains(id)) throw new IllegalStateException("vertex not found");

            Iterator<Map.Entry<Fun.Tuple2<Long,String>,Object>> propsIter =
                    ((NavigableMap)verticesProps).subMap(Fun.t2(id,null),Fun.t2(id,Fun.HI())).entrySet().iterator();

            while(propsIter.hasNext()){
                Map.Entry<Fun.Tuple2<Long,String>,Object> n = propsIter.next();
                if(verticesKeys.contains(n.getKey().b)){
                    verticesIndex.remove(Fun.t3(n.getKey().b,n.getValue(),n.getKey().a));
                }
                propsIter.remove();
            }


            //remove all relevant recids from indexes
            //TODO linear scan, add reverse index
            Iterator<Fun.Tuple4<String,String,Object,Long>> indexIter = verticesIndex2.iterator();
            while(indexIter.hasNext()){
                if(indexIter.next().d.equals(id))
                    indexIter.remove();
            }

            engine.delete(id,VERTEX_SERIALIZER);
            vertices.remove(id);

            //remove related edges
            for(Edge e:getEdges(Direction.OUT))e.remove();
            for(Edge e:getEdges(Direction.IN))e.remove();

        }

        @Override
        public <T> T getProperty(String key) {
            return (T) verticesProps.get(Fun.t2(id, key));
        }

        @Override
        public Set<String> getPropertyKeys() {
            Set<String> ret = new HashSet<String>();
            for(String s:Bind.findVals2(verticesProps.navigableKeySet(), id)){
                ret.add(s);
            }
            return ret;
        }

        @Override
        public void setProperty(String key, Object value) {
            if(key==null||"".equals(key)||"id".equals(key)
                    ||"label".equals(key)) throw new IllegalArgumentException();
            Object oldVal = verticesProps.put(Fun.t2(id,key),value);

            if(verticesKeys.contains(key)){
                //remove old value from index if exists
                if(oldVal!=null) verticesIndex.remove(Fun.t3(key,oldVal,id));
                //put new value
                verticesIndex.add(Fun.t3(key,value,id));
            }
        }

        @Override
        public <T> T removeProperty(String key) {
            T ret = (T) verticesProps.remove(Fun.t2(id, key));
            if(verticesKeys.contains(key)){
                //remove from index
                //remove old value from index if exists
                if(ret!=null) verticesIndex.remove(Fun.t3(key,ret,id));
            }

            return ret;
        }



        @Override
        public Object getId() {
            return id;
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

    protected class MEdge implements Edge{

        protected final Long id;
        protected final long in,out;
        protected final String label;


        public MEdge(Long id, long out, long in,String label) {
            this.id = id;
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
            if(!edges.contains(id)) throw new IllegalStateException("edge not found");

            Iterator<Map.Entry<Fun.Tuple2<Long,String>,Object>> propsIter =
                    ((NavigableMap)edgesProps).subMap(Fun.t2(id,null),Fun.t2(id,Fun.HI())).entrySet().iterator();

            while(propsIter.hasNext()){
                Map.Entry<Fun.Tuple2<Long,String>,Object> n = propsIter.next();
                if(edgesKeys.contains(n.getKey().b)){
                    edgesIndex.remove(Fun.t3(n.getKey().b,n.getValue(),n.getKey().a));
                }
                propsIter.remove();
            }


            //remove all relevant recids from indexes
            //TODO linear scan, add reverse index
            Iterator<Fun.Tuple4<String,String,Object,Long>> indexIter = edgesIndex2.iterator();
            while(indexIter.hasNext()){
                if(indexIter.next().d.equals(id))
                    indexIter.remove();
            }

            engine.delete(id,EDGE_SERIALIZER);
            edges.remove(id);
            edges4vertice.remove(Fun.t4(out,true,label,id));
            edges4vertice.remove(Fun.t4(in,false,label,id));
        }


        @Override
        public <T> T getProperty(String key) {
            return (T) edgesProps.get(Fun.t2(id, key));
        }

        @Override
        public Set<String> getPropertyKeys() {
            Set<String> ret = new HashSet<String>();
            for(String s:Bind.findVals2(edgesProps.navigableKeySet(),id)){
                ret.add(s);
            }
            return ret;
        }

        @Override
        public void setProperty(String key, Object value) {
            if(key==null||"".equals(key)||"id".equals(key)
                    ||"label".equals(key)) throw new IllegalArgumentException();
            Object oldVal = edgesProps.put(Fun.t2(id,key),value);

            if(edgesKeys.contains(key)){
                //remove old value from index if exists
                if(oldVal!=null) edgesIndex.remove(Fun.t3(key,oldVal,id));
                //put new value
                edgesIndex.add(Fun.t3(key,value,id));
            }
        }

        @Override
        public <T> T removeProperty(String key) {
            T ret = (T) edgesProps.remove(Fun.t2(id, key));
            if(edgesKeys.contains(key)){
                //remove from index
                //remove old value from index if exists
                if(ret!=null) edgesIndex.remove(Fun.t3(key,ret,id));
            }

            return ret;
        }



        @Override
        public Object getId() {
            return id;
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
        directory = new File(fileName);
        directory.getParentFile().mkdirs();
        db = DBMaker.newFileDB(directory)
                .asyncWriteDisable()
                //.transactionDisable()
                .make();
        engine = db.getEngine();

        vertices =
                db
                .createTreeSet("vertices")
                .keepCounter(true)
                .serializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .makeOrGet();


        edges = db
                .createTreeSet("edges")
                .keepCounter(true)
                .serializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
                .makeOrGet();

        edges4vertice = db
                .createTreeSet("edges4vertice")
                .serializer(BTreeKeySerializer.TUPLE4)
                .makeOrGet();

        edgesProps = db.createTreeMap("edgesProps")
                .keySerializer(BTreeKeySerializer.TUPLE2)
                .valuesStoredOutsideNodes(true)
                .makeOrGet();

        verticesProps = db.createTreeMap("verticesProps")
                .keySerializer(BTreeKeySerializer.TUPLE2)
                .valuesStoredOutsideNodes(true)
                .makeOrGet();


        verticesIndex = db.createTreeSet("verticesIndex")
                .serializer(BTreeKeySerializer.TUPLE3)
                .makeOrGet();

        edgesIndex = db.createTreeSet("edgesIndex")
                .serializer(BTreeKeySerializer.TUPLE3)
                .makeOrGet();

        verticesKeys = db.createTreeSet("verticesKeys")
                .serializer(BTreeKeySerializer.STRING)
                .makeOrGet();

        edgesKeys = db.createTreeSet("edgesKeys")
                .serializer(BTreeKeySerializer.STRING)
                .makeOrGet();


        verticesIndex2 = db.createTreeSet("verticesIndex2")
                .serializer(BTreeKeySerializer.TUPLE4)
                .makeOrGet();

        edgesIndex2 = db.createTreeSet("edgesIndex2")
                .serializer(BTreeKeySerializer.TUPLE4)
                .makeOrGet();

        verticesKeys2 = db.createTreeSet("verticesKeys2")
                .serializer(BTreeKeySerializer.STRING)
                .makeOrGet();

        edgesKeys2 = db.createTreeSet("edgesKeys2")
                .serializer(BTreeKeySerializer.STRING)
                .makeOrGet();

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
        if(id instanceof String)try{
            id = Long.valueOf((String)id);
        }catch(NumberFormatException e){
            return null;
        }

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

            @Override
            public Iterator<Vertex> iterator() {
                final Iterator<Long> vertIter = vertices.iterator();
                return new MVertexRecidIterator(vertIter);
            }
        };
    }

    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) {

        return new Iterable<Vertex>() {
            @Override
            public Iterator<Vertex> iterator() {

                boolean indexed = verticesKeys.contains(key);

                Set<Long> longs=null;

                if(!indexed){
                    //traverse all
                    longs = new HashSet<Long>();
                    for(Map.Entry<Fun.Tuple2<Long,String>,Object> e:verticesProps.entrySet()){
                        if(e.getKey().b.equals(key)&& e.getValue().equals(value))
                            longs.add(e.getKey().a);
                    }
                }

                final Iterator<Long> i = indexed?
                        Bind.findVals3(verticesIndex,key,value).iterator():longs.iterator();


                return new MVertexRecidIterator(i);
            }
        };

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
        if(id instanceof String)try{
            id = Long.valueOf((String)id);
        }catch(NumberFormatException e){
            return null;
        }

        if(!edges.contains(id))return null;
        return engine.get((Long) id,EDGE_SERIALIZER);
    }

    @Override
    public void removeEdge(Edge edge) {
        edge.remove();
    }


    public class MVertexRecidIterator implements Iterator<Vertex>{
        protected final Iterator<Long> i;

        public MVertexRecidIterator(Iterator<Long> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Vertex next() {
            return engine.get(i.next(), VERTEX_SERIALIZER);
        }

        @Override
        public void remove() {
            i.remove();
        }
    }

    public class MEdgeRecidIterator implements Iterator<Edge>{
        protected final Iterator<Long> i;

        public MEdgeRecidIterator(Iterator<Long> i) {
            this.i = i;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Edge next() {
            return engine.get(i.next(),EDGE_SERIALIZER);
        }

        @Override
        public void remove() {
            i.remove();
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        return new Iterable<Edge>() {

            @Override
            public Iterator<Edge> iterator() {
                return new MEdgeRecidIterator(edges.iterator());
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final String key, final Object value) {

        return new Iterable<Edge>() {
            @Override
            public Iterator<Edge> iterator() {

                boolean indexed = edgesKeys.contains(key);

                Set<Long> longs=null;

                if(!indexed){
                    //traverse all
                    longs = new HashSet<Long>();
                    for(Map.Entry<Fun.Tuple2<Long,String>,Object> e:edgesProps.entrySet()){
                        if(e.getKey().b.equals(key)&& e.getValue().equals(value))
                            longs.add(e.getKey().a);
                    }
                }

                 Iterator<Long> i= indexed?
                        Bind.findVals3(edgesIndex,key,value).iterator():longs.iterator();

                return new MEdgeRecidIterator(i);
            }
        };
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    @Override
    public void shutdown() {
        if(db.isClosed()) return;
        db.commit();
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

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        boolean isVertex = Vertex.class.isAssignableFrom(elementClass);
        (isVertex?verticesKeys:edgesKeys).remove(key);
        Fun.Tuple3 lo = Fun.t3(key, null, null);
        Fun.Tuple3 hi = Fun.t3(key,Fun.HI(),Fun.HI());
        (isVertex?verticesIndex:edgesIndex).subSet(lo, hi).clear();
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        boolean isVertex = Vertex.class.isAssignableFrom(elementClass);


        for(Map.Entry<Fun.Tuple2<Long,String>,Object> e:(isVertex?verticesProps:edgesProps).entrySet()){
            if(e.getKey().b.equals(key)){
                (isVertex?verticesIndex:edgesIndex).add(Fun.t3(key,e.getValue(),e.getKey().a));
            }
        }

        (isVertex?verticesKeys:edgesKeys).add(key);



    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        boolean isVertex = Vertex.class.isAssignableFrom(elementClass);
        return new HashSet<String>(isVertex?verticesKeys:edgesKeys);
    }

    public class MIndex<T extends Element> implements Index<T>{

        protected final String indexName;
        protected final boolean isVertex;

        public MIndex(String indexName, boolean vertex) {
            this.indexName = indexName;
            isVertex = vertex;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public Class<T> getIndexClass() {
            return (Class<T>) (isVertex?Vertex.class:Edge.class);
        }

        @Override
        public void put(String key, Object value, T element) {
            Long recid = (Long) element.getId();
            Fun.Tuple4 t = Fun.t4(indexName,key,value,recid);
            //TODO remove old value?
            (isVertex?verticesIndex2:edgesIndex2).add(t);
        }

        @Override
        public CloseableIterable<T> get(final String key, final Object value) {
            return new CloseableIterable<T>() {
                @Override
                public void close() {
                }

                @Override
                public Iterator<T> iterator() {
                    Iterator<Long> iter = Bind.findVals4(isVertex?verticesIndex2:edgesIndex2,indexName,key,value).iterator();
                    return (Iterator<T>) (isVertex?new MVertexRecidIterator(iter):new MEdgeRecidIterator(iter));
                }
            };
        }

        @Override
        public CloseableIterable<T> query(String key, Object query) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(String key, Object value) {
            Iterator iter=get(key,value).iterator();
            long counter=0;
            while(iter.hasNext()){
                counter++;
                iter.next();
            }
            return counter;
        }

        @Override
        public void remove(String key, Object value, T element) {
            Long recid = (Long) element.getId();
            (isVertex?verticesIndex2:edgesIndex2).remove(Fun.t4(indexName, key, value, recid));
        }
    }

    @Override
    public <T extends Element> Index<T> createIndex(String indexName, Class<T> indexClass, Parameter... indexParameters) {
        if(verticesKeys2.contains(indexName)||edgesKeys2.contains(indexName))
            throw new IllegalArgumentException("Index already exists");
        boolean isVertex = Vertex.class.isAssignableFrom(indexClass);

        (isVertex?verticesKeys2:edgesKeys2).add(indexName);

        return new MIndex<T>(indexName,isVertex);
    }

    @Override
    public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
        boolean isVertex = Vertex.class.isAssignableFrom(indexClass);
        if(!(isVertex?verticesKeys2:edgesKeys2).contains(indexName)) return null;
        return new MIndex<T>(indexName,isVertex);
    }

    @Override
    public Iterable<Index<? extends Element>> getIndices() {
        List ret = new ArrayList();
        for(String s:verticesKeys2) ret.add(new MIndex(s,true));
        for(String s:edgesKeys2) ret.add(new MIndex(s,false));
        return ret;
    }

    @Override
    public void dropIndex(String indexName) {
        //TODO what if there is collision in name on vertex/edge index?
        verticesKeys2.remove(indexName);
        edgesKeys2.remove(indexName);

        Fun.Tuple4 lo = Fun.t4(indexName,null,null,null);
        Fun.Tuple4 hi = Fun.t4(indexName,Fun.HI,Fun.HI,Fun.HI);
        verticesIndex2.subSet(lo,hi).clear();
        edgesIndex2.subSet(lo,hi).clear();
    }

    public String toString() {
       boolean up = !db.isClosed();
       return StringFactory.graphString(this, "vertices:" + (up?this.vertices.size():"CLOSED") + " edges:" +
               (up?this.edges.size():"CLOSED") + " directory:" + this.directory);
    }


}
