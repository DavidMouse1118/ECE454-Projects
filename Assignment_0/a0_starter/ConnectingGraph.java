import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;
    private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
        height = new HashMap<Integer, Integer>();
    }

    public int find(int x) {
        while (x != father.get(x)) {
            // path compression
            father.put(x, father.get(father.get(x)));
            x = father.get(x);
        }
        
        return x;
    }

    public void union(int a, int b) {
        if (!father.containsKey(a)) {
            father.put(a, a);
            height.put(a, 1);
        }

        if (!father.containsKey(b)) {
            father.put(b, b);
            height.put(b, 1);
        }

        int A = find(a);
        int B = find(b);

        // if (A != B) {
            int heightA = height.get(A);
            int heightB = height.get(B);
            if (heightA >= heightB) {
                father.put(B, A);
                height.put(A, heightA + heightB);
            } else {
                father.put(A, B);
                height.put(B, heightA + heightB);
            } 
        // }
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