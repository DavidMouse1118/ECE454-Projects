import java.util.*;

// class to represent Subset 
class subset {
    int parent;
    int rank;
}

class ConnectingGraph {
    private HashMap<Integer, subset> nodeToSubset = null;

    public ConnectingGraph() {
        nodeToSubset = new HashMap<Integer, subset>();
    }

    public int find(int x) {
        if (nodeToSubset.get(x) == null) {
            subset ss = new subset(); 
            ss.parent = x;
            ss.rank = 0;
            nodeToSubset.put(x, ss);
        }

        if (nodeToSubset.get(x).parent != x) {
            nodeToSubset.get(x).parent = find(nodeToSubset.get(x).parent);
        }
        return nodeToSubset.get(x).parent;
    }

    public void union(int x, int y) {
        int xroot = find(x);
        int yroot = find(y);

        if (nodeToSubset.get(xroot).rank < nodeToSubset.get(yroot).rank)
            nodeToSubset.get(xroot).parent = yroot;
        else if (nodeToSubset.get(yroot).rank < nodeToSubset.get(xroot).rank)
            nodeToSubset.get(yroot).parent = xroot;
        else {
            nodeToSubset.get(xroot).parent = yroot;
            nodeToSubset.get(yroot).rank++;
        }
    }

    public HashMap<Integer, subset> getFatherRelation() {
        // Find again for each node
        // for (Map.Entry<Integer, Integer> entry : father.entrySet()) {
        //     int new_value = find(entry.getKey());
        //     father.put(entry.getKey(), new_value);
        // }

        return nodeToSubset;
    }
}