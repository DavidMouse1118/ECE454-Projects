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

import com.google.common.util.concurrent.Striped;

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
    private Striped<Semaphore> stripedSemaphores = Striped.semaphore(64, 1);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupClients = new ConcurrentLinkedQueue<>();
    private int clientNumber = 32;
    
    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        log = Logger.getLogger(KeyValueHandler.class.getName());
        this.primaryAddress = getPrimary();
        this.isPrimary = isPrimary(host, port);
        this.backupClient = getBackupClient();
        this.backupClients = createBackupClients(clientNumber);
        System.out.println(this.backupClients);
        myMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        // System.out.println("Get key = " + key);
        Semaphore stripedSemaphore  = stripedSemaphores.get(key);

        try {
            stripedSemaphore.acquire();

            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } catch (Exception e) {
            e.printStackTrace();
            // throw e;
            return "";
        }finally {
            stripedSemaphore.release();
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        // System.out.println("Put key = " + key + ", value = " + value);
        Semaphore stripedSemaphore  = stripedSemaphores.get(key);

        try {
            stripedSemaphore.acquire();
            myMap.put(key, value);

            if (isPrimary && backupClient != null) {
                writeToBackup(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // throw e;
        } finally {
            stripedSemaphore.release();
        }
    }

	public InetSocketAddress getPrimary() throws Exception {
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public ConcurrentLinkedQueue<KeyValueService.Client> createBackupClients(int clientCounts) throws Exception {
        try {
            ConcurrentLinkedQueue<KeyValueService.Client> backupClients = new ConcurrentLinkedQueue<KeyValueService.Client>();

            curClient.sync();
            List<String> childrenKeys = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
            if (childrenKeys.size() <= 1) {
                log.error("No Backup found");
                return backupClients;
            }
    
            Collections.sort(childrenKeys);
            byte[] data = curClient.getData().forPath(zkNode + "/" + childrenKeys.get(1));
            String strData = new String(data);
            String[] backupData = strData.split(":");
            String backupHost = backupData[0];
            int backupPort = Integer.parseInt(backupData[1]);
    
            log.info("Found backup " + strData);
            log.info("Setting up backup client");
    
            for(int i = 0; i < clientCounts; i++) {
                TSocket sock = new TSocket(backupHost, backupPort);
                TTransport transport = new TFramedTransport(sock);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
        
                backupClients.add(new KeyValueService.Client(protocol));
            }
    
            return backupClients;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public KeyValueService.Client getBackupClient() throws Exception {
        try {
            if (!isPrimary) {
                return null;
            }
    
            curClient.sync();
            List<String> childrenKeys = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
            if (childrenKeys.size() <= 1) {
                log.error("No Backup found");
                return null;
            }
    
            Collections.sort(childrenKeys);
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
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void writeToBackup(String key, String value) throws Exception {
        try {
            KeyValueService.Client currentBackupClient = null;

            while(currentBackupClient == null) {
                currentBackupClient = backupClients.poll();
            }

            currentBackupClient.put(key, value);
            backupClients.add(currentBackupClient);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void copyData(Map<String, String> data) throws org.apache.thrift.TException {
        lock.writeLock().lock();

        try {
            if (myMap == null) {
                myMap = data;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

	synchronized public void process(WatchedEvent event) {
        lock.writeLock().lock();

		log.info("ZooKeeper event " + event);
		try {
            primaryAddress = getPrimary();
            isPrimary = isPrimary(host, port);
            backupClient = getBackupClient();

            if (backupClient != null) {
                backupClient.copyData(myMap);
                backupClients = createBackupClients(clientNumber);
                System.out.println(this.backupClients);
                System.out.println(clientNumber + " backup clients are created.");
            }
		} catch (Exception e) {
            log.error("Unable to determine primary or children");
		} finally {
            lock.writeLock().unlock();
        }
    }

    public Boolean isPrimary(String host, int port) {
        return primaryAddress != null && host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort();
    }
}
