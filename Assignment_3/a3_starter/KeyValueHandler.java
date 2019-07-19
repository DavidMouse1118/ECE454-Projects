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

    private static Logger log;
    private volatile Boolean isPrimary = false;
    private ReentrantLock globalLock = new ReentrantLock();
    private Striped<Lock> stripedLock = Striped.lock(64);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupClients = null;
    private int clientNumber = 32;
    
    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;

        log = Logger.getLogger(KeyValueHandler.class.getName());
        // Set up watcher
        curClient.getChildren().usingWatcher(this).forPath(zkNode);

        myMap = new ConcurrentHashMap<String, String>();
    }

    // There is no need to lock the get operation
    public String get(String key) throws org.apache.thrift.TException {
        if (isPrimary == false) {
            // System.out.println("Backup is not allowed to get.");
            throw new org.apache.thrift.TException("Backup is not allowed to get.");
        }

        try {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        if (isPrimary == false) {
            // System.out.println("Backup is not allowed to put.");
            throw new org.apache.thrift.TException("Backup is not allowed to put.");
        }

        // Key level locking 
        Lock lock = stripedLock.get(key);
        lock.lock();

        // Check global lock. Prevent put operation during copying the data
        while (globalLock.isLocked());

        try {
            // Save data to local primary
            myMap.put(key, value);

            // has backup clients
            if (this.backupClients != null) {
                // writeToBackup
                KeyValueService.Client currentBackupClient = null;

                while(currentBackupClient == null) {
                    currentBackupClient = backupClients.poll();
                }
    
                currentBackupClient.putBackup(key, value);

                this.backupClients.offer(currentBackupClient);
            }
        } catch (Exception e) {
            e.printStackTrace();
            this.backupClients = null;
        } finally {
            lock.unlock();
        }
    }

    public void putBackup(String key, String value) throws org.apache.thrift.TException {
        // // Key level locking 
        Lock lock = stripedLock.get(key);
        lock.lock();

        try {
            myMap.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
    
    public void copyData(Map<String, String> data) throws org.apache.thrift.TException {
        this.myMap = new ConcurrentHashMap<String, String>(data); 
        System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< Copy Data to backup Succeeded!");
    }
    
	synchronized public void process(WatchedEvent event) throws org.apache.thrift.TException {
        // Lock the entire hashmap on primary
        try {
            // Get all the children
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            
            // Find primary data and backup data
            Collections.sort(children);
            byte[] primaryData = null;
            byte[] backupData = null;
            
            if (children.size() > 2) {
                // System.out.println("There are more than 2 nodes.");
                primaryData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 2));
                backupData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            } else {
                primaryData = curClient.getData().forPath(zkNode + "/" + children.get(0));
                if (children.size() == 2) {
                    backupData = curClient.getData().forPath(zkNode + "/" + children.get(1));
                }
            }
            
            String strPrimaryData = new String(primaryData);
            String[] primary = strPrimaryData.split(":");
            String primaryHost = primary[0];
            int primaryPort = Integer.parseInt(primary[1]);
            
            // Check if this is primary
            if (primaryHost.equals(host) && primaryPort == port) {
                // System.out.println("Is Primary: " + true);
                this.isPrimary = true;
            } else {
                // System.out.println("Is Primary: " + false);
                this.isPrimary = false;
            }
            
            
            if (this.isPrimary && backupData != null) {
                System.out.println("Copying Data to backup >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                // System.out.println("Does have backup clients.");
                String strBackupData = new String(backupData);
                String[] backup = strBackupData.split(":");
                String backupHost = backup[0];
                int backupPort = Integer.parseInt(backup[1]);

                // Create first backup client for data transfer
                KeyValueService.Client firstBackupClient = null;

                while(firstBackupClient == null) {
                    try {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        firstBackupClient = new KeyValueService.Client(protocol);
                    } catch (Exception e) {
                        // System.out.println("First backup client failed. Retrying ...");
                    }
                }
                
                // Copy data to backup
                globalLock.lock();
                
                firstBackupClient.copyData(this.myMap);

                // Create 32 backup clients
                this.backupClients = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
                for(int i = 0; i < clientNumber; i++) {
                    TSocket sock = new TSocket(backupHost, backupPort);
                    TTransport transport = new TFramedTransport(sock);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
            
                    this.backupClients.add(new KeyValueService.Client(protocol));
                }
                globalLock.unlock();
            } else {
                // System.out.println("Does not have backup clients.");
                this.backupClients = null;
            }
        } catch (Exception e) {
            log.error("Unable to determine primary or children");
            this.backupClients = null;
        }
    }
}
