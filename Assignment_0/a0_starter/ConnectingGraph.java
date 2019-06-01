import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;
    private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
        height = new HashMap<Integer, Integer>();
    }

    public void add(int node) {
        if (!father.containsKey(node)){
            father.put(node, node);
            height.put(node, 0);
        }
    }

    public int find(int node) {
        // find root father
        while (father.get(node) != node) {
            node = father.get(node);
        }

        return node;
    }

    public void union(int node1, int node2) {
        // Add nodes if not exist
        add(node1);
        add(node2);

        // Find the root father
        int f1 = find(node1);
        int f2 = find(node2);

        // Connect root father based on its height
        if (f1 != f2) {
            int h1 = height.get(f1);
            int h2 = height.get(f2);

            if (h1 > h2) {
                father.put(f2, f1);
            } else if (h1 < h2) {
                father.put(f1, f2);
            } else {
                father.put(f1, f2);
                height.put(f2, h2 + 1);
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Integer, Integer> entry : father.entrySet()) {
            sb.append(entry.getKey());
            sb.append(" ");
            sb.append(find(entry.getKey())); // find and add the root father
            sb.append("\n");
        }

        return sb.toString();
    }
}