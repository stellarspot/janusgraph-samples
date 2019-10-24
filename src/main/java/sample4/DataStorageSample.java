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

    public static void main(String[] args) throws Exception {

        try (JanusGraphStorage storage = getInMemoryStorage();
             JanusGraphStorageTransaction tx = storage.tx()) {

            RawNode rawNode = new RawNode("Node", "value");
            Node node = tx.getNode(rawNode);
            System.out.printf("node: %s%n", node);

            RawLink rawLink = new RawLink("Link",
                    new RawNode("Node1", "value1"),
                    new RawNode("Node2", "value2"));

            Link link = tx.getLink(rawLink);
            System.out.printf("link: %s%n", link);

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

        public RawLink(String type, RawAtom... atoms) {
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

        public Link(long id, String type, long... ids) {
            super(id, type);
            this.ids = ids;
        }

        public int getArity() {
            return ids.length;
        }

        public Atom getAtom(JanusGraphStorageTransaction tx, int index) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        @Override
        public String toString() {
            return String.format("%s(%s) - Link[%d]", type, Arrays.toString(ids), id);
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

        public Link getLink(RawLink link) {

            Vertex v = g
                    .inject("nothing")
                    .union(getOrCreateLink(link))
                    .next();

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

            Function<Traverser<Object>, Iterator<String>> mapIds = t -> {
                ArrayList arrayList = (ArrayList) t.get();
                long[] ids = new long[arrayList.size()];
                for (int i = 0; i < arrayList.size(); i++) {
                    ids[i] = (long) arrayList.get(i);
                }
                List<String> list = new ArrayList<>(1);
                list.add(idsToString(ids));
                return list.iterator();
            };

            GraphTraversal<Object, Vertex> addVertex = union(getOrCreateAtoms(link))
                    .id()
                    .fold()
                    .as("ids")
                    .addV(LABEL_LINK)
                    .property(KIND, LABEL_LINK)
                    .property(TYPE, link.type)
                    .property(IDS, select("ids").flatMap(mapIds))
                    .property(T.id, storage.getNextId());

            return union(getOrCreateAtoms(link))
                    .id()
                    .fold()
                    .as("ids")
                    .V()
                    .hasLabel(LABEL_LINK)
                    .has(KIND, LABEL_LINK)
                    .has(TYPE, link.type)
                    .has(IDS, select("ids").flatMap(mapIds))
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


        static long id(Vertex v) {
            return (long) v.id();
        }

        static long[] ids(Vertex v) {
            String ids = v.property(IDS).value().toString();
            return toIds(ids);
        }
    }

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
}
