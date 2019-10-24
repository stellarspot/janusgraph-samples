package sample2;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataStorageSample {

    private static JanusGraph getInMemoryGraph() {
        JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "inmemory")
                .set("graph.set-vertex-id", "true")
                .set("ids.authority.wait-time", "5")
                .set("ids.renew-timeout", "50")
                .set("ids.block-size", "1000000000")
                .set("ids.renew-percentage", "0.3")
                //.set("query.force-index", true)
                .open();
        return graph;
    }

    private static JanusGraphStorage getInMemoryStorage() {
        return new JanusGraphStorage(getInMemoryGraph());
    }

    public static void main(String[] args) throws Exception {

        long time = System.currentTimeMillis();
        JanusGraph graph = getInMemoryGraph();
        System.out.printf("elapsed time: %dms%n", System.currentTimeMillis() - time);
        graph.close();
    }

    public static void main2(String[] args) throws Exception {

        int N = 1;

        System.out.printf("elements: %d%n", N);

        DataGenerator generator = new DataGenerator(3, 3, 3, 3, N);
//        generator.dump();

        long time = System.currentTimeMillis();
        try (JanusGraphStorage storage = getInMemoryStorage()) {

            generator.upload(storage);

            System.out.printf("elapsed time: %dms%n", System.currentTimeMillis() - time);
            storage.printStatistics();
        }

    }


    static class JanusGraphStorage implements Closeable {

        long currentId = 0;
        final JanusGraph graph;
        final IDManager idManager;

        public JanusGraphStorage(JanusGraph graph) {
            this.graph = graph;
            this.idManager = ((StandardJanusGraph) graph).getIDManager();
//            makeIndices();
        }

        public GraphTraversalSource traversal() {
            return graph.traversal();
        }

        public void commit() {
            graph.tx().commit();
        }

        public Vertex getLeaf(GraphTraversalSource g, String type, String value) {

            GraphTraversal<Vertex, Vertex> iter = g
                    .V()
                    .hasLabel("Leaf")
                    .has("type", type)
                    .has("value", value);

            if (iter.hasNext()) {
                return iter.next();
            }

            return g
                    .addV("Leaf")
                    .property(T.id, getNextId())
                    .property("type", type)
                    .property("value", value).next();
        }

        public Vertex getNode(GraphTraversalSource g, String type, Vertex... children) {

            long[] ids = getIds(children);

            GraphTraversal<Vertex, Vertex> iter = g
                    .V()
                    .hasLabel("Node")
                    .has("type", type)
                    .has("ids", ids);

            if (iter.hasNext()) {
                return iter.next();
            }

            Vertex vertex = g
                    .addV("Node")
                    .property(T.id, getNextId())
                    .property("type", type)
                    .property("arity", children.length)
                    .property("ids", ids).next();

            for (int i = 0; i < children.length; i++) {
                String key = getKey(type, children.length, i);
                children[i].addEdge(key, vertex);
            }

            return vertex;
        }

        private void makeIndices() {

            JanusGraphManagement mgmt = graph.openManagement();
            createIndex(mgmt, "leafIndex", "Leaf", "type", "value");
            createIndex(mgmt, "nodeIndex", "Node", "type", "ids");
            mgmt.commit();
        }

        private static void createIndex(JanusGraphManagement mgmt, String indexName, String label, String... keys) {

            if (mgmt.getGraphIndex(indexName) == null) {
                JanusGraphManagement.IndexBuilder builder = mgmt
                        .buildIndex(indexName, Vertex.class)
                        .indexOnly(mgmt.getOrCreateVertexLabel(label));

                for (String key : keys) {
                    builder = builder.addKey(mgmt.getOrCreatePropertyKey(key));
                }

                builder.buildCompositeIndex();
            }
        }

        long getNextId() {
            return idManager.toVertexId(++currentId);
        }

        @Override
        public void close() throws IOException {
            graph.close();
        }

        public void printStatistics() {
            GraphTraversalSource g = graph.traversal();
            long vertices = g.V().count().next();
            long edges = g.E().count().next();
            System.out.printf("vertices: %d, edges: %d%n", vertices, edges);
        }

        private String getKey(String type, int arity, int position) {
            return String.format("%s_%d_%d", type, arity, position);
        }

        private static long[] getIds(Vertex... vertices) {
            long[] ids = new long[vertices.length];

            for (int i = 0; i < vertices.length; i++) {
                ids[i] = (long) vertices[i].id();
            }
            return ids;
        }
    }

    static class DataGenerator {

        final int maxTypes;
        final int maxValues;
        final int maxWidth;
        final int maxHeight;
        final int elements;
        final Random random = new Random(42);

        final List<DataNode> dataNodes;

        public DataGenerator(int maxTypes, int maxValues, int maxWidth, int maxHeight, int elements) {
            this.maxTypes = maxTypes;
            this.maxValues = maxValues;
            this.maxWidth = maxWidth;
            this.maxHeight = maxHeight;
            this.elements = elements;
            this.dataNodes = new ArrayList<>(elements);

            init();
        }

        private void init() {
            for (int i = 0; i < elements; i++) {
                dataNodes.add(generateNode(maxWidth, maxHeight));
            }
        }

        public void upload(JanusGraphStorage storage) {

            GraphTraversalSource g = storage.traversal();


            for (DataNode node : dataNodes) {
                upload(storage, g, node);
            }

            storage.commit();
        }

        private Vertex upload(JanusGraphStorage storage, GraphTraversalSource g, DataNode node) {

            if (node.isLeaf()) {
                return storage.getLeaf(g, node.type, node.value);
            }

            DataNode[] children = node.children;
            Vertex[] vertices = new Vertex[children.length];

            for (int i = 0; i < children.length; i++) {
                vertices[i] = upload(storage, g, children[i]);
            }

            return storage.getNode(g, node.type, vertices);
        }


        private DataNode generateNode(int width, int depth) {

            if (depth == 0) {
                return new DataNode(getLeafType(), getValue());
            }

            int currentWidth = random.nextInt(width) + 1;
            int currentDepth = random.nextInt(depth) + 1;


            DataNode[] children = new DataNode[currentWidth];

            for (int i = 0; i < currentWidth; i++) {
                children[i] = generateNode(currentWidth, currentDepth - 1);
            }

            return new DataNode(getNodeType(), children);
        }


        private String getLeafType() {
            return withRandomPostfix("Leaf");
        }

        private String getNodeType() {
            return withRandomPostfix("Node");
        }

        private String getValue() {
            return withRandomPostfix("Value");
        }

        private String withRandomPostfix(String name) {
            return String.format("%s%d", name, random.nextInt(maxTypes));
        }

        public void dump() {
            System.out.printf("--- dump ---%n");
            System.out.printf("data nodes: %d%n", dataNodes.size());
            for (DataNode node : dataNodes) {
                System.out.printf("%s%n", node);
            }
            System.out.printf("--- ---- ---%n");
        }
    }

    static class DataNode {
        final String type;
        final String value;
        final DataNode[] children;

        public DataNode(String type, DataNode... children) {
            this(type, null, children);
        }

        public DataNode(String type, String value, DataNode... children) {
            this.type = type;
            this.value = value;
            this.children = children;
        }

        public boolean isLeaf() {
            return children.length == 0;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            toString(builder, "");
            builder.append('\n');
            return builder.toString();
        }

        public void toString(StringBuilder builder, String indent) {
            builder
                    .append("\n")
                    .append(indent)
                    .append(type)
                    .append("(");
            if (children.length == 0) {
                builder
                        .append("'")
                        .append(value)
                        .append("'");
            } else {
                String nextIndent = indent + " ";
                for (DataNode node : children) {
                    node.toString(builder, nextIndent);
                }
            }
            builder.append(")");
        }
    }
}
