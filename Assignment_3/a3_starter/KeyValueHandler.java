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

/*
Performance:
    1000 strips, 8 client thread, 100 key size, 120s:
        Aggregate throughput: 135181 RPCs/s
        Average latency: 0.05 ms

    64 strips, 8 client thread, 100 key size, 120s:
        Aggregate throughput: 136929 RPCs/s
        Average latency: 0.05 ms

    1000 strips, 8 client thread, 1000 key size, 120s:
        Aggregate throughput: 135181 RPCs/s
        Average latency: 0.05 ms
*/

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private static Logger log;
    private volatile Boolean isPrimary;
    private KeyValueService.Client backupClientForCopy = null;
    private ReentrantLock lock = new ReentrantLock();
    private Striped<Semaphore> stripedSemaphores = Striped.semaphore(64, 1);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupClients = null;
    private int clientNumber = 32;
    
    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;

        log = Logger.getLogger(KeyValueHandler.class.getName());
        this.isPrimary = isPrimary(host, port);
        // this.backupClients = createBackupClients(clientNumber);
        // System.out.println(this.backupClients);
        // System.out.println(clientNumber + " backup clients are created.");

        myMap = new ConcurrentHashMap<String, String>();
    }

    // There is no need to lock the get operation
    public String get(String key) throws IllegalArgument, org.apache.thrift.TException {
        // System.out.println("Get key = " + key);
        if (isPrimary == false) {
            System.out.println("Backup is not allowed to get.");
            throw new IllegalArgument("Backup is not allowed to get.");
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

    public void put(String key, String value) throws IllegalArgument, org.apache.thrift.TException {
        // System.out.println("Put key = " + key + ", value = " + value);
        if (isPrimary == false) {
            System.out.println("Backup is not allowed to put.");
            throw new IllegalArgument("Backup is not allowed to put.");
        }

        // Check global lock. Prevent put operation during copying the data
        boolean isGlobalLocked = true;

        while (isGlobalLocked) {
            isGlobalLocked = lock.isLocked();
        }

        // Key level locking 
        Semaphore stripedSemaphore  = stripedSemaphores.get(key);

        try {
            stripedSemaphore.acquire();
            // Save data to local
            myMap.put(key, value);

            // Is primary and has backup clients
            if (isPrimary && backupClients != null) {
                writeToBackup(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stripedSemaphore.release();
        }
    }

    public void putBackup(String key, String value) throws IllegalArgument, org.apache.thrift.TException {
        // System.out.println("Put key = " + key + ", value = " + value);
        // Check global lock. Prevent put operation during copying the data
        boolean isGlobalLocked = true;

        while (isGlobalLocked) {
            isGlobalLocked = lock.isLocked();
        }

        // Key level locking 
        Semaphore stripedSemaphore  = stripedSemaphores.get(key);

        try {
            stripedSemaphore.acquire();
            // Save data to local
            myMap.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stripedSemaphore.release();
        }
    }

    public void writeToBackup(String key, String value) throws Exception {
        try {
            KeyValueService.Client currentBackupClient = null;

            while(currentBackupClient == null) {
                currentBackupClient = backupClients.poll();
            }

            currentBackupClient.putBackup(key, value);
            backupClients.add(currentBackupClient);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

	public Boolean isPrimary(String host, int port) throws Exception {
        try {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

            if (children.size() == 0) {
                // log.error("No primary found");
                return false;
            }
    
            Collections.sort(children);
            byte[] data;

            if (children.size() > 2) {
                System.out.println("There are 3 node");
                data = curClient.getData().forPath(zkNode + "/" + children.get(1));
            } else {
                data = curClient.getData().forPath(zkNode + "/" + children.get(0));
            }

            String strData = new String(data);
            // log.info("Found primary " + strData);
            String[] primary = strData.split(":");
            String primaryHost = primary[0];
            int primaryPort = Integer.parseInt(primary[1]);

            Boolean isPrimary = false;

            if (primaryHost.equals(host) && primaryPort == port) {
                isPrimary = true;
            }

            // log.info("Is Primary: " + isPrimary);

            return isPrimary;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
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
    }

    public ConcurrentLinkedQueue<KeyValueService.Client> createBackupClients(int clientNumber) throws Exception {
        try {
            if (!isPrimary) {
                // System.out.println("This is Backup. No need to create backup client.");
                return null;
            }

            curClient.sync();
            List<String> childrenKeys = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
            if (childrenKeys.size() <= 1) {
                // log.info("This is primary. But there is no Backup found");
                return null;
            }
    
            Collections.sort(childrenKeys);
            byte[] data;

            if (childrenKeys.size() > 2) {
                System.out.println("There are 3 node");
                data = curClient.getData().forPath(zkNode + "/" + childrenKeys.get(2));
            } else {
                data = curClient.getData().forPath(zkNode + "/" + childrenKeys.get(1));
            }

            String strData = new String(data);
            String[] backupData = strData.split(":");
            String backupHost = backupData[0];
            int backupPort = Integer.parseInt(backupData[1]);
    
            // log.info("This is primary. Found backup " + strData);
            // log.info("Setting up " + clientNumber + " backup clients.");

            ConcurrentLinkedQueue<KeyValueService.Client> backupClients = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
            for(int i = 0; i < clientNumber; i++) {
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

    public void copyData(Map<String, String> data) throws IllegalArgument, org.apache.thrift.TException {
        // Lock the entire hashmap on backup
        lock.lock();

        try {
            this.myMap = new ConcurrentHashMap<String, String>(data);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            lock.unlock();
        }
    }

	synchronized public void process(WatchedEvent event) {
        // log.info("ZooKeeper event " + event);
        lock.lock();
		try {
            isPrimary = isPrimary(host, port);
            backupClientForCopy = getBackupClient();

            if (backupClientForCopy != null) {
                backupClientForCopy.copyData(this.myMap);
                System.out.println("Copy Data to backup Succeeded!");

                backupClients = createBackupClients(clientNumber);
                System.out.println("BackupClients: " + this.backupClients);
                System.out.println(clientNumber + " backup clients are created.");
            }
		} catch (Exception e) {
            // log.error("Unable to determine primary or children");
		} finally {
            lock.unlock();
        }
    }
}
