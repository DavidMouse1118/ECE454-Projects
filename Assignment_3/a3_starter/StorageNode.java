import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
	static Logger log;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		CuratorFramework curClient = CuratorFrameworkFactory.builder().connectString(args[2])
				.retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(1000).sessionTimeoutMs(10000).build();

		curClient.start();
		curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(args[3] + "/ChildNode", (args[0] + ":" + args[1]).getBytes());

		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(
				new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]));
		TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				// connect to 
				server.serve();
			}
		}).start();

		new Thread(new Runnable(){
			public void run() {
				String zkNode = args[3];

				try {
					List<String> children = new ArrayList<>();
					while (children.size() == 0) {
						curClient.sync();
						children = curClient.getChildren().forPath(zkNode);
					}

					// If only primary
					if (children.size() == 1) {
						return;
					} 

					// If has backup, get backup data 
					Collections.sort(children);
					byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
					String strBackupData = new String(backupData);
					String[] backup = strBackupData.split(":");
					String backupHost = backup[0];
					int backupPort = Integer.parseInt(backup[1]);

					// Ping primary if this is backup
					// if (backupHost.equals(args[0]) && backupPort == Integer.parseInt(args[1])) {
						// get primary data
						byte[] primaryData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 2));
						String strPrimaryData = new String(primaryData);
						String[] primary = strPrimaryData.split(":");
						String primaryHost = primary[0];
						int primaryPort = Integer.parseInt(primary[1]);
						
						
						System.out.println("Try to ping primary...");
						TSocket sock = new TSocket(primaryHost, primaryPort);
						TTransport transport = new TFramedTransport(sock);
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						KeyValueService.Client primaryClient = new KeyValueService.Client(protocol);
						
						while (true) {
							try {
								Thread.sleep(50);
								primaryClient.setPrimary(true);	// Won't change anything
								// Primary alive 
								continue;
							} catch (Exception e) {
								System.out.println("Backup loose connection to Primary");
								// Cannot connect to Primary
								break;
							}
						}

						//Primary is dead. Delete primary znode
						System.out.println("Delete primary znode");
						curClient.delete().forPath(zkNode + "/" + children.get(children.size() - 2));

						// Backup set itself as Primary
						System.out.println("Backup becomes the primary");
						sock = new TSocket(backupHost, backupPort);
						transport = new TFramedTransport(sock);
						transport.open();
						protocol = new TBinaryProtocol(transport);
						KeyValueService.Client BackupClient = new KeyValueService.Client(protocol);
						
						BackupClient.setPrimary(true);
					// }
					// Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		}).start();
	}
}