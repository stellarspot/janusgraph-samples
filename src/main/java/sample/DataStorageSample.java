package sample;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class DataStorageSample {

    public static void main(String[] args) throws Exception {

        try (JanusGraphSorage storage = getInMemoryStorage()) {

            DataGenerator generator = new DataGenerator(3, 3, 3, 3, 3);
            generator.dump();

            long time = System.currentTimeMillis();
            generator.upload(storage);
            System.out.printf("elapsed time: %dms%n", System.currentTimeMillis() - time);


        }
    }


    public static JanusGraphSorage getInMemoryStorage() {
        JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "inmemory")
                //.set("graph.set-vertex-id", "true")
                //.set("query.force-index", true)
                .open();
        return new JanusGraphSorage(graph);
    }
}
