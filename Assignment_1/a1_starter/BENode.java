import java.net.InetAddress;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TProtocol;
import java.util.concurrent.CountDownLatch;
import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;



public class BENode {
	static Logger log;
	// test
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: java BENode FE_host FE_port BE_port");
			System.exit(-1);
		}

		// initialize log4j
		try {
			BasicConfigurator.configure();
			log = Logger.getLogger(BENode.class.getName());

			String hostFE = args[0];
			int portFE = Integer.parseInt(args[1]);
			int portBE = Integer.parseInt(args[2]);
			log.info("Launching BE node on port " + portBE + " at host " + getHostName());

			// launch Thrift server
			BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(
					new BEHandler());
			TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
			THsHaServer.Args sargs = new THsHaServer.Args(socket);
			sargs.protocolFactory(new TBinaryProtocol.Factory());
			sargs.transportFactory(new TFramedTransport.Factory());
			sargs.processorFactory(new TProcessorFactory(processor));
			// sargs.maxWorkerThreads(64);
			THsHaServer server = new THsHaServer(sargs);

			// Send heartbeat to FE
			sendHeartBeatToClient(getHostName(), portBE, hostFE, portFE);
			System.out.println("Successfully send heart beat to client");

			server.serve();
			System.out.println("should appear");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	static void sendHeartBeatToClient(String beHost, int bePort, String feHost, int fePort) {
		boolean heartBeatSuccess = false;

		while (!heartBeatSuccess) {
			try {
				TSocket sock = new TSocket(feHost, fePort);
				TTransport transport = new TFramedTransport(sock);
				TProtocol protocol = new TBinaryProtocol(transport);
				BcryptService.Client client = new BcryptService.Client(protocol);
				transport.open();
				client.beToFeRegistrar(beHost, bePort);
				transport.close();
				heartBeatSuccess = true;
			} catch (Exception e) {
				// Do nothing
			}
		}	
	}

	static String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
	}
}
