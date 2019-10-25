package sample4;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class DataStorageSample {

    private static final boolean DEBUG = false;

    public static void main(String[] args) throws Exception {

        try (JanusGraphStorage storage = getInMemoryStorage();
             JanusGraphStorageTransaction tx = storage.tx()) {

            // try to change Link1 type to Link2
            final RawLink rawLink = new RawLink("Link1",
                    new RawLink("Link2",
                            new RawNode("Node1", "value1")),
                    new RawLink("Link3",
                            new RawNode("Node2", "value2")));

            Link link = tx.getLink(rawLink);
            System.out.printf("raw     link: %s%n", rawLink);
            System.out.printf("storage link: %s%n", link);

            tx.dump();
            tx.commit();
        }
    }

    private static JanusGraph getInMemoryGraph() {
        return JanusGraphFactory.build()
                .set("storage.backend", "inmemory")
                .set("graph.set-vertex-id", "true")
                .open();
    }

    private static JanusGraphStorage getInMemoryStorage() {
        return new JanusGraphStorage(getInMemoryGraph());
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
        static final String KIND = "prop_kind";
        static final String TYPE = "prop_type";
        static final String VALUE = "prop_value";
        static final String IDS = "prop_ids";

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
            GraphTraversal<String, Vertex> traversal = g
                    .inject("nothing")
                    .union(getOrCreateNode(node));

            if (DEBUG) {
                System.out.printf("get node: %s%n", traversal);
            }

            Vertex v = traversal.next();
            return new Node(id(v), node.type, node.value);
        }

        public Link getLink(RawLink link) {

            GraphTraversal<String, Vertex> traversal = g
                    .inject("nothing")
                    .union(getOrCreateLink(link));

            if (DEBUG) {
                System.out.printf("get link: %s%n", traversal);
            }

            Vertex v = traversal.next();
            return new Link(id(v), link.type, ids(v));
        }

        public void commit() {
            tx.commit();
        }

        @Override
        public void close() {
            tx.close();
        }

        private GraphTraversal<Object, Vertex> getOrCreateAtom(RawAtom atom) {
            if (atom instanceof RawNode) {
                return getOrCreateNode((RawNode) atom);
            } else if (atom instanceof RawLink) {
                return getOrCreateLink((RawLink) atom);
            } else {
                String msg = String.format("Unknown RawAtom class: %s", atom.getClass());
                throw new RuntimeException(msg);
            }
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

        private GraphTraversal<Object, Vertex> getOrCreateLink(RawLink link) {

            GraphTraversal<Object, Vertex> addVertex = union(getOrCreateAtoms(link))
                    .id()
                    .fold()
                    .as("ids")
                    .addV(LABEL_LINK)
                    .property(KIND, LABEL_LINK)
                    .property(TYPE, link.type)
                    .property(IDS, select("ids").flatMap(MAP_IDS))
                    .property(T.id, storage.getNextId());

            return union(getOrCreateAtoms(link))
                    .id()
                    .fold()
                    .as("ids")
                    .V()
                    .hasLabel(LABEL_LINK)
                    .has(KIND, LABEL_LINK)
                    .has(TYPE, link.type)
                    .has(IDS, select("ids").flatMap(MAP_IDS))
                    .fold()
                    .coalesce(unfold(), addVertex);
        }

        private GraphTraversal<Object, Vertex>[] getOrCreateAtoms(RawLink link) {
            int arity = link.getArity();
            GraphTraversal<Object, Vertex>[] addAtoms = new GraphTraversal[arity];

            for (int i = 0; i < arity; i++) {
                addAtoms[i] = getOrCreateAtom(link.atoms[i]);
            }
            return addAtoms;
        }

        public void dump() {
            System.out.printf("--- Storage Dump ---%n");
            Iterator<Vertex> vertices = g.V();
            while (vertices.hasNext()) {
                Vertex v = vertices.next();
                String kind = v.property(KIND).value().toString();
                String type = v.property(TYPE).value().toString();
                Object id = v.id();
                if (LABEL_NODE.equals(kind)) {
                    String value = v.property(VALUE).value().toString();
                    System.out.printf("%s[%s]: %s(%s)%n", kind, id, type, value);
                } else {
                    System.out.printf("%s[%s]: %s(%s)%n", kind, id, type, Arrays.toString(ids(v)));
                }
            }
            System.out.printf("--- ------------ ---%n");
        }

        static long id(Vertex v) {
            return (long) v.id();
        }

        static long[] ids(Vertex v) {
            String ids = v.property(IDS).value().toString();
            return toIds(ids);
        }
    }

    static final Function<Traverser<Object>, Iterator<String>> MAP_IDS = t -> {
        ArrayList arrayList = (ArrayList) t.get();
        long[] ids = new long[arrayList.size()];
        for (int i = 0; i < arrayList.size(); i++) {
            ids[i] = (long) arrayList.get(i);
        }
        List<String> list = new ArrayList<>(1);
        list.add(idsToString(ids));
        return list.iterator();
    };

    static String idsToString(long... ids) {
        StringBuilder builder = new StringBuilder();
        for (long id : ids) {
            builder.append(id).append(':');
        }
        return builder.toString();
    }

    static long[] toIds(String str) {
        String[] split = str.split(":");

        long[] ids = new long[split.length];

        for (int i = 0; i < split.length; i++) {
            ids[i] = Long.parseLong(split[i]);
        }
        return ids;
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

        public RawLink(String type, RawAtom... atoms) {
            super(type);
            this.atoms = atoms;
        }

        public int getArity() {
            return atoms.length;
        }

        @Override
        public String toString() {
            return String.format("%s(%s)", type, Arrays.toString(atoms));
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

        public Link(long id, String type, long... ids) {
            super(id, type);
            this.ids = ids;
        }

        @Override
        public String toString() {
            return String.format("Link[%d]: %s(%s)", id, type, Arrays.toString(ids));
        }
    }
}
