import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import java.util.concurrent.CountDownLatch;
import org.apache.thrift.*;
import org.apache.thrift.async.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;

public class FENode {
	static Logger log;

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: java FENode FE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(FENode.class.getName());

		int portFE = Integer.parseInt(args[0]);
		log.info("Launching FE node on port " + portFE);

		// launch Thrift server
		BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(
				new FEHandler());
		TNonblockingServerSocket socket = new TNonblockingServerSocket(portFE);
		THsHaServer.Args sargs = new THsHaServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		TServer server = new THsHaServer(sargs);
		server.serve();
	}

}
