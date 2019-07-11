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

public class CreateZNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	BasicConfigurator.configure();
	log = Logger.getLogger(StorageNode.class.getName());

	if (args.length != 2) {
	    System.err.println("Usage: java CreateZKNode zkconnectstring zknode");
	    System.exit(-1);
	}

	CuratorFramework curClient =
	    CuratorFrameworkFactory.builder()
	    .connectString(args[0])
	    .retryPolicy(new RetryNTimes(10, 1000))
	    .connectionTimeoutMs(1000)
	    .sessionTimeoutMs(10000)
	    .build();

	curClient.start();
	Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
		    curClient.close();
		}
	    });

	ZKPaths.mkdirs(curClient.getZookeeperClient().getZooKeeper(), args[1]);
    }
}
