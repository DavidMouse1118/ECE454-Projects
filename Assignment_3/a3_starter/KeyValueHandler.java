import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.*;

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

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private InetSocketAddress primaryAddress;
    private List<InetSocketAddress> children;
    private static Logger log;
    private Boolean isPrimary;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        log = Logger.getLogger(KeyValueHandler.class.getName());
        this.primaryAddress = getPrimary();
        this.isPrimary = isPrimary(host, port);
        this.children = getChildren();
        myMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        // System.out.println("Get key = " + key);
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        // System.out.println("Put key = " + key + ", value = " + value);
        myMap.put(key, value);

        try {
            if (isPrimary) {
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

    public List<InetSocketAddress> getChildren() throws Exception {
        List<InetSocketAddress> children = new ArrayList<>();

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
            
            children.add(new InetSocketAddress(childHost, childPort));
        }

        return children;
    }

    public void writeToBackup(String key, String value) throws Exception {
        try {
            if (children.size() == 0) {
                return;
            }

            for (InetSocketAddress child: children) {
				TSocket sock = new TSocket(child.getHostName(), child.getPort());
				TTransport transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				KeyValueService.Client client =  new KeyValueService.Client(protocol);
                client.put(key, value);
                // transport.close();
            }
        } catch (TTransportException e) {
            return;
        }
    }

	synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		try {
            primaryAddress = getPrimary();
            children = getChildren();
            this.isPrimary = isPrimary(host, port);
		} catch (Exception e) {
			log.error("Unable to determine primary or children");
		}
    }

    public Boolean isPrimary(String host, int port) {
        return primaryAddress != null && host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort();
    }
}
