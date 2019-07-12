import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private volatile InetSocketAddress primaryAddress;
    private volatile List<KeyValueService.Client> children;
    private static Logger log;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception{
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        log = Logger.getLogger(KeyValueHandler.class.getName());
        this.primaryAddress = getPrimary();
        this.children = getChildren();
        myMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        System.out.println("Get key: " + key);
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        System.out.println("Put key = " + key + ", value = " + value);
        myMap.put(key, value);

        try {
            if (isPrimary(host, port)) {
                // log.info("It is the primary server. Writing to backup server ...");
                writeToBackup(key, value);
            }
        } catch (Exception e) {
            return;
        }
    }

	public InetSocketAddress getPrimary() throws Exception {
        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
        if (children.size() == 0) {
            log.error("No primary found");
            return null;
        }
        Collections.sort(children);
        byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
        String strData = new String(data);
        String[] primary = strData.split(":");
        log.info("Found primary " + strData);
        return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
    }
    
    public List<KeyValueService.Client> getChildren() throws Exception {
        List<KeyValueService.Client> children = new ArrayList<>();

        if (primaryAddress != null && !isPrimary(host, port)) {
            return children;
        }

        curClient.sync();
        List<String> childrenKeys = curClient.getChildren().usingWatcher(this).forPath(zkNode);
        if (childrenKeys.size() == 0) {
            log.error("No children found");
            Thread.sleep(100);
            return children;
        }
        
        for (String childkey: childrenKeys) {
            byte[] data = curClient.getData().forPath(zkNode + "/" + childkey);
            String strData = new String(data);
            String[] childData = strData.split(":");
            String childHost = childData[0];
            int childPort = Integer.parseInt(childData[1]);

            if (isPrimary(childHost, childPort)) {
                continue;
            }

            log.info("Found child " + strData);
            log.info("Connecting to child " + strData);
            

            try {
				TSocket sock = new TSocket(childHost, childPort);
				TTransport transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				children.add(new KeyValueService.Client(protocol));
			} catch (Exception e) {
				log.error("Unable to connect to child " + strData);
            }
            
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
        }

        return children;
    }

    public void writeToBackup(String key, String value) throws Exception {
        try {
            if (children.size() == 0) {
                return;
            }

            for (KeyValueService.Client child: children) {
                child.put(key, value);
            }
        } catch (Exception e) {
            return;
        }
    }

	synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		try {
            primaryAddress = getPrimary();
            children = getChildren();
		} catch (Exception e) {
			log.error("Unable to determine primary or children");
		}
    }
    
    public Boolean isPrimary(String host, int port) {
        return host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort();
    }
}
