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

        //waitForProfiler("start profiler and press enter");
        long time = System.currentTimeMillis();
        try (JanusGraphSorage storage = getInMemoryStorage()) {

            DataGenerator generator = new DataGenerator(3, 3, 3, 3, N);
            //generator.dump();

            generator.upload(storage);
        }

        System.out.printf("elapsed time: %dms%n", System.currentTimeMillis() - time);
    }

    public static JanusGraphSorage getInMemoryStorage() {
        JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "inmemory")
                .set("graph.set-vertex-id", "true")
                .set("ids.block-size", "100000")
                .set("ids.authority.wait-time", "5")
                .set("ids.renew-timeout", "50")
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
