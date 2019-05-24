import java.util.*;

class ConnectingGraph {
    private HashMap<Integer, Integer> father = null;

    public ConnectingGraph() {
        father = new HashMap<Integer, Integer>();
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
            father.put(A, B);
        }
    }

}