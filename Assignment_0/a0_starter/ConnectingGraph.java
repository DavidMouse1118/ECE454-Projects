import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;
    private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
        height = new HashMap<Integer, Integer>();
    }

    public int find(int x) {
        if (father[x] == x) {
            return x
        }

        // path compression
        return father[x] = find(father[x])
    }

    public void connect(int a, int b) {
        int A = find(a);
        int B = find(b);
        
        if (A != B) {
            if (height.getOrDefault(A, 1) > height.getOrDefault(B, 1)) {
                father.put(B, A);
            } else {
                father.put(A, B);
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