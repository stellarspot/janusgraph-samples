package sample;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {

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

    public void upload(JanusGraphSorage storage) {

        GraphTraversalSource g = storage.traversal();

        for (DataNode node : dataNodes) {
            upload(storage, g, node);
        }

        storage.commit();

        storage.printStatistics(g);
    }

    private Vertex upload(JanusGraphSorage storage, GraphTraversalSource g, DataNode node) {

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
