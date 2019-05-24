import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

class ConnectingGraph2 {
    private HashMap<Integer, Integer> father = null;
    // private HashMap<Integer, Integer> height = null;

    public ConnectingGraph2() {
        father = new HashMap<Integer, Integer>();
        // height = new HashMap<Integer, Integer>();
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

    public void union(int a, int b) {
        int A = find(a);
        int B = find(b);

        if (A != B) {
            // int heightA = height.getOrDefault(A, 0);
            // int heightB = height.getOrDefault(B, 0);
            // if (heightA > heightB) {
            //     father.put(B, A);
            // } else if (heightA < heightB) {
            //     father.put(A, B);
            // } else {
            father.put(A, B);
            //     height.put(B, heightB + 1);
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


    public static void main(String[] args) throws FileNotFoundException {
        File file=new File("Assignment_0/a0_starter/sample_input/huge.txt");
        Scanner scanner = new Scanner(file);
        ConnectingGraph2 graph = new ConnectingGraph2();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] nodes = line.split("\\s");
            int node0 = Integer.parseInt(nodes[0].trim());
            int node1 = Integer.parseInt(nodes[1].trim());
//            System.out.println(node1);

            graph.union(node0, node1);
        }
        scanner.close();
    }
}
