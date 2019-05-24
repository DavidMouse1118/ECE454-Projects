import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;
    private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
        height = new HashMap<Integer, Integer>();
    }

    public int find(int x) {
        int j, fx;
        j = x;

        if (father.containsKey(j) == false) {
            father.put(j, j);
            return j;
        }
        
        // find x 的 big brother
        while (father.get(j) != j) {
            j = father.get(j);
        }
        
        // path compression
        while (x != j) {
            fx = father.get(x);
            father.put(x, j);
            x = fx;
        }
        
        return j;
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