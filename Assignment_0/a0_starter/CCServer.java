import java.io.*;
import java.net.*;

class CCServer {
	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: java CCServer port");
			System.exit(-1);
		}
		int port = Integer.parseInt(args[0]);

		ServerSocket ssock = new ServerSocket(port);
		System.out.println("listening on port " + port);
		Socket csock = ssock.accept();
		System.out.println("Accepted connections: " + csock);
		while (true) {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(csock.getInputStream()));

				PrintWriter writer = new PrintWriter(csock.getOutputStream(), true);

				String line = null;
				System.out.println(line);
			

				//writer.println(line.toUpperCase());
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
