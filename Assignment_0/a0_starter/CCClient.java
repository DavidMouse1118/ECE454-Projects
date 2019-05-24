import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.net.*;
 
class CCClient {
    public static void main(String args[]) throws Exception {
	if (args.length != 4) {
	    System.out.println("usage: java CCClient host port input_file output_file");
	    System.exit(-1);
	}
	String host = args[0];
	int port = Integer.parseInt(args[1]);
	String inputFileName = args[2];
	String outputFileName = args[3];

	// step 1: read graph into byte array
	String input = new String(Files.readAllBytes(Paths.get(inputFileName)), StandardCharsets.UTF_8);
	System.out.println("read input from " + inputFileName);

	// step 2: connect to server
	System.out.println("connecting to " + host + ":" + port);
	Socket sock = new Socket(host, port);
	System.out.println("connected, sending request");

	// step 3: send reqest to server
	DataOutputStream dout = new DataOutputStream(sock.getOutputStream());
	byte[] bytes = input.getBytes("UTF-8");
	long startTime = System.currentTimeMillis();
	dout.writeInt(bytes.length);
	dout.write(bytes);
	dout.flush();
	System.out.println("sent request header and " + bytes.length + " bytes of payload data to server");

	// step 4: receive response from server
	DataInputStream din = new DataInputStream(sock.getInputStream());
	int respDataLen = din.readInt();
	System.out.println("received response header, data payload has length " + respDataLen);
	bytes = new byte[respDataLen];
	din.readFully(bytes);
	long endTime = System.currentTimeMillis();
	System.out.println("received " + bytes.length + " bytes of payload data from server in " + (endTime - startTime) + "ms");
	String output = new String(bytes, StandardCharsets.UTF_8);

	// step 5: save to file
	Files.write(Paths.get(outputFileName), output.getBytes("UTF-8"));
	System.out.println("wrote output to " + outputFileName);
	
	// step 6: clean up
	sock.close();
	System.out.println("terminated connection to server");

	// note: you should keep the connection open and reuse it
	//       if sending multiple requests back-to-back
    }
}
