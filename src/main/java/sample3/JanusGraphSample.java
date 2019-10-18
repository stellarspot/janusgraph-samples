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

public class JanusGraphSample {

    // --- Parameters for test ---
    private static final int VERTICES = 10;
    private static final int ITERATIONS = 10;
    private static final boolean USE_CUSTOM_IDS = true;
    // --- ------------------- ---

    private static final boolean DEBUG = false;

    private static final String LABEL = "SampleLabel";
    private static final String KEY = "SampleKey";
    private static final String VALUE = "SampleValue";

    public static void main(String[] args) {

        MeasuredTime[] times = new MeasuredTime[ITERATIONS];
        for (int i = 0; i < ITERATIONS; i++) {
            MeasuredTime measuredTime = testJanusGraph(USE_CUSTOM_IDS);
            times[i] = measuredTime;
        }

        printStatistics(times);
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
        final long insertTime;

        public MeasuredTime(long elapsedTime, long creationTime, long insertTime) {
            this.elapsedTime = elapsedTime;
            this.creationTime = creationTime;
            this.insertTime = insertTime;
        }

        @Override
        public String toString() {
            return String.format(
                    "elapsed time:  %d(ms), graph creation time: %d, vertices insertion time: %d(ms)",
                    elapsedTime, creationTime, insertTime);
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
            long insertTime = elapsedTime - creationTime;
            elapsedTime = elapsedTime - time;
            creationTime = creationTime - time;

            MeasuredTime measuredTime = new MeasuredTime(elapsedTime, creationTime, insertTime);
            if (DEBUG) {
                printDebugMessage(graph, measuredTime);
            }

            return measuredTime;
        }
    }

    private static void printStatistics(MeasuredTime[] times) {

        System.out.printf("Vertices: %d%n", VERTICES);
        System.out.printf("First call:%n");
        System.out.printf("%s%n", times[0]);

        double totalElapsedTime = 0;
        double totalCreationTime = 0;
        double totalInsertTime = 0;

        for (int i = 1; i < times.length; i++) {
            MeasuredTime measuredTime = times[i];
            totalElapsedTime += measuredTime.elapsedTime;
            totalCreationTime += measuredTime.creationTime;
            totalInsertTime += measuredTime.insertTime;
        }

        // exclude first item
        int size = times.length - 1;

        System.out.printf("Other calls:%n");
        System.out.printf("Average time elapsed: %.2f, creation: %.2f, insertion: %f.2%n",
                totalElapsedTime / size, totalCreationTime / size, totalInsertTime / size);

    }

    private static void printDebugMessage(JanusGraph graph, MeasuredTime measuredTime) {
        long vertices = verticesCount(graph);
        System.out.printf("[custom ids] vertices: %d," +
                        " elapsed time:  %d(ms)," +
                        " graph creation time: %d" +
                        " vertices insertion time: %d(ms)%n",
                vertices, measuredTime.elapsedTime, measuredTime.creationTime, measuredTime.insertTime);
    }
}