import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

class CCServer {
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

				// Initialing connecting graph
				ConnectingGraph graph = new ConnectingGraph();
				System.out.println("Connecting graph has been initialized.");
				
				// Read node from file using byte, and connect two nodes
				int i = 0;
				while (i < bytes.length) {
					int node1 = 0;
					while (bytes[i] != 32) {
						char c = (char) bytes[i];
						node1 = node1 * 10 + Character.getNumericValue(c);
						i++;
					}

					System.out.println(node1);

					i++;
					int node2 = 0;
					while (bytes[i] != 10) {
						char c = (char) bytes[i];
						node2 = node2 * 10 + Character.getNumericValue(c);
						i++;
					}
					System.out.println(node2);
					i++;

					graph.union(node1, node2);
				} 

				// Output connected components
				Map<Integer, Integer> node_to_father = graph.getFatherRelation();
				String result = "";

				for (Map.Entry<Integer, Integer> entry : node_to_father.entrySet()) {
					result += entry.getKey() + " " + entry.getValue() + "\n";
				}

				System.out.println(node_to_father);

				// Write result to the client
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
