import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;
    private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
        height = new HashMap<Integer, Integer>();
    }

    public int find(int x) {
        if (father.get(x) == null) {
            father.put(x, x);
            return x;
        }

        if (father.get(x) == x) {
            return x;
        }

        // path compression
        int root_father = find(father.get(x));

        father.put(x, root_father);

        return root_father;
    }

    public void connect(int a, int b) {
        int A = find(a);
        int B = find(b);
        
        if (A != B) {
            int heightA = height.getOrDefault(A, 1);
            int heightB = height.getOrDefault(B, 1);
            if (heightA > heightB) {
                father.put(B, A);
                height.put(A, heightA + heightB);
            } else {
                father.put(A, B);
                height.put(B, heightA + heightB);
            }
        }
    }

    public HashMap<Integer, Integer> getFatherRelation() {
        for (Map.Entry<Integer, Integer> entry : father.entrySet()) {
            int new_value = find(entry.getKey());
            father.put(entry.getKey(), new_value);
        }
        
        return father;
    }
}