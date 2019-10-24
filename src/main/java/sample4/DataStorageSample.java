package sample4;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class DataStorageSample {

    public static void main(String[] args) throws Exception {

        try (JanusGraphStorage storage = getInMemoryStorage();
             JanusGraphStorageTransaction tx = storage.tx()) {

            RawNode rawNode = new RawNode("Node", "value");
            Node node = tx.getNode(rawNode);
            System.out.printf("node: %s%n", node);

            tx.commit();
        }
    }

    private static JanusGraph getInMemoryGraph() {
        JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "inmemory")
                .set("graph.set-vertex-id", "true")
                //.set("query.force-index", true)
                .open();
        return graph;
    }

    private static JanusGraphStorage getInMemoryStorage() {
        return new JanusGraphStorage(getInMemoryGraph());
    }

    // Raw Atoms
    static class RawAtom {
        final String type;

        public RawAtom(String type) {
            this.type = type;
        }
    }

    static class RawNode extends RawAtom {
        final String value;

        public RawNode(String type, String value) {
            super(type);
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%s(%s)", type, value);
        }
    }

    static class RawLink extends RawAtom {
        final RawAtom[] atoms;

        public RawLink(String type, RawAtom[] atoms) {
            super(type);
            this.atoms = atoms;
        }

        public int getArity() {
            return atoms.length;
        }
    }

    // Atoms in JanusGraph Storage

    static class Atom {
        final long id;
        final String type;

        public Atom(long id, String type) {
            this.id = id;
            this.type = type;
        }
    }

    static class Node extends Atom {
        final String value;

        public Node(long id, String type, String value) {
            super(id, type);
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%s(%s) - Node[%d]", type, value, id);
        }
    }

    static class Link extends Atom {

        final long[] ids;

        public Link(long id, String type, long[] ids) {
            super(id, type);
            this.ids = ids;
        }

        public int getArity() {
            return ids.length;
        }

        public Atom getAtom(JanusGraphStorageTransaction tx, int index) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }
    }

    // JanusGraph Storage

    static class JanusGraphStorage implements Closeable {

        long currentId = 0;
        final JanusGraph graph;
        final IDManager idManager;

        public JanusGraphStorage(JanusGraph graph) {
            this.graph = graph;
            this.idManager = ((StandardJanusGraph) graph).getIDManager();
        }

        public JanusGraphStorageTransaction tx() {
            return new JanusGraphStorageTransaction(this);
        }

        @Override
        public void close() {
            graph.close();
        }

        public long getNextId() {
            return idManager.toVertexId(++currentId);
        }
    }

    static class JanusGraphStorageTransaction implements Closeable {

        // "type" is a reserved property name in JanusGraph
        static final String KIND = "as_kind";
        static final String TYPE = "as_type";
        static final String VALUE = "as_value";
        static final String IDS = "as_ids";

        static final String LABEL_NODE = "Node";
        static final String LABEL_LINK = "Link";

        final JanusGraphStorage storage;
        final JanusGraphTransaction tx;
        final GraphTraversalSource g;

        public JanusGraphStorageTransaction(JanusGraphStorage storage) {
            this.storage = storage;
            this.tx = storage.graph.newTransaction();
            this.g = tx.traversal();
        }


        public Node getNode(RawNode node) {
            Vertex v = g
                    .inject("nothing")
                    .union(getOrCreateNode(node))
                    .next();
            return new Node(id(v), node.type, node.value);
        }

        public void commit() {
            tx.commit();
        }

        @Override
        public void close() {
            tx.close();
        }

        private GraphTraversal<Object, Vertex> getOrCreateNode(RawNode node) {

            GraphTraversal<Object, Vertex> addVertex = addV(LABEL_NODE)
                    .property(T.id, storage.getNextId())
                    .property(KIND, LABEL_NODE)
                    .property(TYPE, node.type)
                    .property(VALUE, node.value);

            return V()
                    .hasLabel(LABEL_NODE)
                    .has(TYPE, node.type)
                    .has(VALUE, node.value)
                    .fold()
                    .coalesce(unfold(), addVertex);
        }

        static long id(Vertex v) {
            return (long) v.id();
        }
    }

}
