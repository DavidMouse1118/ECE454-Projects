import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.locks.*;
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
    private static Logger log;
    private Boolean isPrimary;
    private KeyValueService.Client backupClient;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        log = Logger.getLogger(KeyValueHandler.class.getName());
        this.primaryAddress = getPrimary();
        this.isPrimary = isPrimary(host, port);
        this.backupClient = getBackupClient();
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
        lock.writeLock().lock();
        // System.out.println("Put key = " + key + ", value = " + value);
        myMap.put(key, value);

        try {
            if (isPrimary && backupClient != null) {
                writeToBackup(key, value);
            }
        } catch (Exception e) {
            return;
        } finally {
            lock.writeLock().unlock();
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

    public KeyValueService.Client getBackupClient() throws Exception {
        if (!isPrimary) {
            return null;
        }

        curClient.sync();
        List<String> childrenKeys = curClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (childrenKeys.size() <= 1) {
            log.error("No Backup found");
            return null;
        }
        
        byte[] data = curClient.getData().forPath(zkNode + "/" + childrenKeys.get(1));
        String strData = new String(data);
        String[] backupData = strData.split(":");
        String backupHost = backupData[0];
        int backupPort = Integer.parseInt(backupData[1]);

        log.info("Found backup " + strData);
        log.info("Setting up backup client");
        
        TSocket sock = new TSocket(backupHost, backupPort);
        TTransport transport = new TFramedTransport(sock);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);

        return new KeyValueService.Client(protocol);
    }

    public void writeToBackup(String key, String value) throws Exception {
        lock.writeLock().lock();

        try {
            backupClient.put(key, value);
        } catch (TTransportException e) {
            return;
        } finally {
            lock.writeLock().unlock();
        }
    }

	synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		try {
            primaryAddress = getPrimary();
            isPrimary = isPrimary(host, port);
            backupClient = getBackupClient();
		} catch (Exception e) {
			log.error("Unable to determine primary or children");
		}
    }

    public Boolean isPrimary(String host, int port) {
        return primaryAddress != null && host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort();
    }
}
