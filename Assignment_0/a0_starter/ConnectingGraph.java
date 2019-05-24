import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;
    // private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
        // height = new HashMap<Integer, Integer>();
    }

    public int find(int x) {
        int j, fx;
        j = x;
        
        // find x's root father
        while (father.get(j) != j) {
            j = father.get(j);
        }

        // // path compression
        // while (x != j) {
        // fx = father.get(x);
        // father.put(x, j);
        // x = fx;
        // }

        return j;
    }

    public void union(int a, int b) {
        if (!father.containsKey(a)) {
            father.put(a, a);
        }

        if (!father.containsKey(b)) {
            father.put(b, b);
        }
        int A = find(a);
        int B = find(b);

        if (A != B) {
            // int heightA = height.getOrDefault(A, 0);
            // int heightB = height.getOrDefault(B, 0);
            // if (heightA > heightB) {
            // father.put(B, A);
            // } else if (heightA < heightB) {
            // father.put(A, B);
            // } else {
            father.put(A, B);
            // height.put(B, heightB + 1);
            // }
        }
    }

    public HashMap<Integer, Integer> getFatherRelation() {
        // Find again for each node
        for (Map.Entry<Integer, Integer> entry : father.entrySet()) {
            int new_value = find(entry.getKey());
            father.put(entry.getKey(), new_value);
        }

        return father;
    }
}