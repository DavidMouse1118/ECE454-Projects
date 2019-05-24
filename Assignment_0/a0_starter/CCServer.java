import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

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
				bytes = new byte[respDataLen];
				din.readFully(bytes);
				long endTime = System.currentTimeMillis();
				System.out.println(
						"received " + bytes.length + " bytes of payload data from server in " + (endTime - startTime) + "ms");
				String output = new String(bytes, StandardCharsets.UTF_8);

				System.out.println(output);

				

				// ConnectingGraph graph = new ConnectingGraph();
				// System.out.println("Connecting graph has been initialized.");

				// BufferedReader reader = new BufferedReader(new InputStreamReader(csock.getInputStream(), "UTF-8"));

				// PrintWriter writer = new PrintWriter(csock.getOutputStream(), true);

				// String line = null;
				// while ((line = reader.readLine()) != null) {
				// 	// System.out.println(line);
				// 	// process line to integers
				// 	String[] nodes = line.split("\\s");
				// 	int node0 = Integer.parseInt(nodes[0].trim());
				// 	int node1 = Integer.parseInt(nodes[1].trim());

				// 	graph.connect(node0, node1);
				// 	System.out.println(line);
				// }

				// System.out.println(123);
				// Map<Integer, Integer> node_to_father = graph.getFatherRelation();

				// System.out.println(node_to_father);
				// for (Map.Entry<Integer, Integer> entry : node_to_father.entrySet()) {
				// 	System.out.println(entry.getKey() + " -> " + entry.getValue());
				// }





				// writer.println(line.toUpperCase());
				// csock.close();


				/*
				 * YOUR CODE GOES HERE - accept connection from server socket - read requests
				 * from connection repeatedly - for each request, compute an output and send a
				 * response - each message has a 4-byte header followed by a payload - the
				 * header is the length of the payload (signed, two's complement, big-endian) -
				 * the payload is a string (UTF-8, big-endian)
				 */
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
