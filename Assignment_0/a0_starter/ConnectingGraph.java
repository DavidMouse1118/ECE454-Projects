import java.util.*;

class ConnectingGraph {
    private int[] father = null;
    // private HashMap<Integer, Integer> father = null;
    // private HashMap<Integer, Integer> height = null;

    public ConnectingGraph() {
        father = new int[Integer.MAX_VALUE];
        // father = new HashMap<Integer, Integer>();
        // height = new HashMap<Integer, Integer>();
    }

    public int find(int x) {
        int j, fx;
        j = x;

        // // find x's root father
        // while (father.get(j) != j) {
        //     j = father.get(j);
        // }

        // // // path compression
        // // while (x != j) {
        // //     fx = father.get(x);
        // //     father.put(x, j);
        // //     x = fx;
        // // }

        return j;
    }

    public void union(int a, int b) {
        if (father[a] == 0) {
            father[a] = a;
            // height.put(a, 0);
        }
        if (father[b] == 0) {
            father[b] = b;
            // height.put(a, 0);
        }
        // if (father.get(a) == null) {
        //     father.put(a, a);
        //     // height.put(a, 0);
        // }

        // if (father.get(b) == null) {
        //     father.put(b, b);
        //     // height.put(b, 0);
        // }

        int A = find(a);
        int B = find(b);

        return;

        // if (A != B) {
        //     int heightA = height.get(A);
        //     int heightB = height.get(B);

        //     if (heightA > heightB) {
        //         father.put(B, A);
        //     } else if (heightA < heightB) {
        //         father.put(A, B);
        //     } else {
        //         father.put(A, B);
        //         height.put(B, heightB + 1);
        //     }
        // }
    }

    public int[] refreshFatherRelation() {
        // Find again for each node
        // for (Map.Entry<Integer, Integer> entry : father.entrySet()) {
        //     int new_value = find(entry.getKey());
        //     father.put(entry.getKey(), new_value);
        // }

        return father;
    }
}