import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

class CCServer {
    public int find(HashMap<Integer, Integer> father, int x) {
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

    public void union(HashMap<Integer, Integer> father, int a, int b) {
        int A = find(father, a);
        int B = find(father, b);
        
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
	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: java CCServer port");
			System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);

		ServerSocket ssock = new ServerSocket(port);
		System.out.println("listening on port " + port);
		while (true) {
			try {
				Socket csock = ssock.accept();
				System.out.println("Accepted connections: " + csock);

				DataInputStream din = new DataInputStream(csock.getInputStream());
				int respDataLen = din.readInt();
				System.out.println("received response header, data payload has length " + respDataLen);
				byte[] bytes = new byte[respDataLen];
				din.readFully(bytes);
				String output = new String(bytes, StandardCharsets.UTF_8);

				private HashMap<Integer, Integer> father = new HashMap<Integer, Integer>();;
				System.out.println("Connecting graph has been initialized.");

				// Read edges and union
				Scanner scanner = new Scanner(output);
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					String[] nodes = line.split("\\s");
					int node0 = Integer.parseInt(nodes[0].trim());
					int node1 = Integer.parseInt(nodes[1].trim());

					union(father, node0, node1);
				}
				scanner.close();

				// output connected components
				// Map<Integer, Integer> node_to_father = graph.getFatherRelation();
				String result = "";
				
				for (Map.Entry<Integer, Integer> entry : father.entrySet()) {
					root = find(father, entry.getKey());
					result += entry.getKey() + " " + root + "\n";
				}

				// System.out.println(result);

				DataOutputStream dout = new DataOutputStream(csock.getOutputStream());
				bytes = result.getBytes("UTF-8");
				dout.writeInt(bytes.length);
				dout.write(bytes);
				dout.flush();
				System.out.println("sent result header and " + bytes.length + " bytes of payload data to Client");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
