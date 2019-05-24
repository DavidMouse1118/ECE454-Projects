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
				String output = new String(bytes, StandardCharsets.UTF_8);

				ConnectingGraph graph = new ConnectingGraph();
				System.out.println("Connecting graph has been initialized.");

				Scanner scanner = new Scanner(output);
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					String[] nodes = line.split("\\s");
					int node0 = Integer.parseInt(nodes[0].trim());
					int node1 = Integer.parseInt(nodes[1].trim());

					graph.connect(node0, node1);
					System.out.println(line);
				}
				scanner.close();

				Map<Integer, Integer> node_to_father = graph.getFatherRelation();
				String result = "";
				
				for (Map.Entry<Integer, Integer> entry : node_to_father.entrySet()) {
					result += entry.getKey() + " " + entry.getValue() + "\n";
				}

				System.out.println(result);

				DataOutputStream dout = new DataOutputStream(csock.getOutputStream());
				bytes = result.getBytes("UTF-8");
				dout.writeInt(bytes.length);
				dout.write(bytes);
				dout.flush();
				System.out.println("sent result header and " + bytes.length + " bytes of payload data to Client");


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
