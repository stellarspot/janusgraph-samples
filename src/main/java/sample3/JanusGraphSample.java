package sample3;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;

import java.util.ArrayList;
import java.util.List;

public class JanusGraphSample {

    private static final String LABEL = "SampleLabel";
    private static final String KEY = "SampleKey";
    private static final String VALUE = "SampleValue";
    private static final int VERTICES = 1000;
    private static final int ITERATIONS = 100;
    private static final boolean DEBUG = true;

    public static void main(String[] args) {

        List<MeasuredTime> times = new ArrayList<>(ITERATIONS);
        for (int i = 0; i < ITERATIONS; i++) {
            MeasuredTime measuredTime = testJanusGraph(false);
            times.add(measuredTime);
        }
    }

    private static JanusGraph getJanusGraph(boolean customIds) {

        JanusGraphFactory.Builder builder = JanusGraphFactory.build()
                .set("storage.backend", "inmemory")
                .set("ids.authority.wait-time", "5")
                .set("ids.renew-timeout", "50")
                .set("ids.block-size", "1000000000")
                //.set("cluster.max-partitions", "2")
                .set("ids.renew-percentage", "0.3");

        if (customIds) {
            builder = builder.set("graph.set-vertex-id", "true");
        }

        return builder.open();
    }

    static long verticesCount(JanusGraph graph) {
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource g = tx.traversal();
            return g.V().count().next();
        }
    }

    static class MeasuredTime {
        final long elapsedTime;
        final long creationTime;

        public MeasuredTime(long elapsedTime, long creationTime) {
            this.elapsedTime = elapsedTime;
            this.creationTime = creationTime;
        }
    }

    public static MeasuredTime testJanusGraph(boolean customIds) {

        long time = System.currentTimeMillis();
        try (JanusGraph graph = getJanusGraph(customIds)) {

            IDManager idManager = null;

            if (customIds) {
                idManager = ((StandardJanusGraph) graph).getIDManager();
            }

            long creationTime = System.currentTimeMillis();

            try (JanusGraphTransaction tx = graph.newTransaction()) {
                GraphTraversalSource g = tx.traversal();

                for (int i = 1; i <= VERTICES; i++) {
                    String value = String.format("%s-%d", VALUE, i);
                    GraphTraversal<Vertex, Vertex> traversal = g
                            .addV(LABEL)
                            .property(KEY, value);

                    if (customIds) {
                        long id = idManager.toVertexId(i);
                        traversal = traversal.property(T.id, id);
                    }

                    traversal.next();
                }
                tx.commit();
            }
            long elapsedTime = System.currentTimeMillis();
            elapsedTime = elapsedTime - time;
            creationTime = creationTime - time;

            if (DEBUG) {
                printDebugMessage(graph, elapsedTime, creationTime);
            }

            return new MeasuredTime(elapsedTime, creationTime);
        }
    }

    private static void printDebugMessage(JanusGraph graph, long elapsedTime, long creationTime) {
        long vertices = verticesCount(graph);
        System.out.printf("[custom ids] vertices: %d, elapsed time:  %d(ms), graph creation time: %d%n",
                vertices, elapsedTime, creationTime);
    }
}