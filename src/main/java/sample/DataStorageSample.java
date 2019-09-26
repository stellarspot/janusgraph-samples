package sample;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.Scanner;

public class DataStorageSample {

    public static void main(String[] args) throws Exception {

        int N = 10;

        if (args.length > 0) {
            N = Integer.parseInt(args[0]);
        }

        System.out.printf("elements: %d%n", N);

        try (JanusGraphSorage storage = getInMemoryStorage()) {

            DataGenerator generator = new DataGenerator(3, 3, 3, 3, N);
            generator.dump();

            //waitForProfiler("start profiler and press enter");
            long time = System.currentTimeMillis();
            generator.upload(storage);
            System.out.printf("elapsed time: %dms%n", System.currentTimeMillis() - time);
            //waitForProfiler("stop profiler and press enter");
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

    public static void waitForProfiler(String msg) {
        System.out.println(msg);
        Scanner in = new Scanner(System.in);
        in.nextLine();
    }

}
