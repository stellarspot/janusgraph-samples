package sample;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.io.Closeable;
import java.io.IOException;

public class JanusGraphSorage implements Closeable {

    long currentId = 0;
    final JanusGraph graph;
    final IDManager idManager;

    public JanusGraphSorage(JanusGraph graph) {
        this.graph = graph;
        this.idManager = ((StandardJanusGraph) graph).getIDManager();
        makeIndices();
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

    public void printStatistics(GraphTraversalSource g) {
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
