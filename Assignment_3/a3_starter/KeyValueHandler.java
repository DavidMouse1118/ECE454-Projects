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
    private volatile Boolean isPrimary = false;
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
        // Set up watcher
        // copyDataFromPrimaryOnStartup();
        // this.isPrimary = isPrimary(host, port);
        // this.backupClients = createBackupClients(clientNumber);
        // System.out.println(this.backupClients);
        // System.out.println(clientNumber + " backup clients are created.");

        // myMap = new ConcurrentHashMap<String, String>();
    }

    public void copyDataFromPrimaryOnStartup() throws org.apache.thrift.TException {
        try {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
            if (children.size() == 0) {
                log.error("No data found");
                log.info("Is primary: " + true);
                this.isPrimary = true;
                this.myMap = new ConcurrentHashMap<String, String>();
                return;
            } 

            log.info("Is primary: " + false);
            this.isPrimary = false;
    
            Collections.sort(children);
            byte[] primaryData;

            // if (children.size() > 1) {
            //     System.out.println("There are more than 2 existing nodes.");
            //     primaryData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            // } else {
            //     primaryData = curClient.getData().forPath(zkNode + "/" + children.get(0));
            // }
            primaryData = curClient.getData().forPath(zkNode + "/" + children.get(0));

            String strData = new String(primaryData);
            log.info("Found primary " + strData);
            String[] primary = strData.split(":");
            String primaryHost = primary[0];
            int primaryPort = Integer.parseInt(primary[1]);

            TSocket sock = new TSocket(primaryHost, primaryPort);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client primaryClient = new KeyValueService.Client(protocol);

            this.myMap = new ConcurrentHashMap<String, String>(primaryClient.getPrimaryData());
            System.out.println(this.myMap);
            System.out.println("Copy Data on startup Succeeded!");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getPrimaryData() throws org.apache.thrift.TException {
        lock.lock();

        try {
            return this.myMap;
        } finally {
            lock.unlock();
        }
    }

    // There is no need to lock the get operation
    public String get(String key) throws org.apache.thrift.TException {
        if (isPrimary == false) {
            System.out.println("Backup is not allowed to get.");
            throw new org.apache.thrift.TException("Backup is not allowed to get.");
        }
        // System.out.println("Primary: Get key = " + key);
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
            System.out.println("Backup is not allowed to put.");
            throw new org.apache.thrift.TException("Backup is not allowed to put.");
        }
        // System.out.println("Primary: Put key = " + key + ", value = " + value);

        // Key level locking 
        Semaphore stripedSemaphore  = stripedSemaphores.get(key);

        try {
            stripedSemaphore.acquire();

            // Check global lock. Prevent put operation during copying the data
            boolean isGlobalLocked = true;
            while (isGlobalLocked) {
                isGlobalLocked = lock.isLocked();
            }

            // Save data to local primary
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

    public void putBackup(String key, String value) throws org.apache.thrift.TException {
        // System.out.println("Backup: Put key = " + key + ", value = " + value);

        // // Key level locking 
        // Semaphore stripedSemaphore  = stripedSemaphores.get(key);

        try {
            // stripedSemaphore.acquire();
            
            // // Check global lock. Prevent put operation during copying the data
            // boolean isGlobalLocked = true;

            // while (isGlobalLocked) {
            //     isGlobalLocked = lock.isLocked();
            // }

            // Save data to local backup
            myMap.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // stripedSemaphore.release();
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

	// public Boolean isPrimary(String host, int port) throws Exception {
    //     try {
    //         curClient.sync();
    //         List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
    //         if (children.size() == 0) {
    //             // log.error("No primary found");
    //             return false;
    //         }
    
    //         Collections.sort(children);
    //         byte[] data;

    //         if (children.size() > 2) {
    //             System.out.println("There are more than 2 nodes.");
    //             data = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 2));
    //         } else {
    //             data = curClient.getData().forPath(zkNode + "/" + children.get(0));
    //         }

    //         String strData = new String(data);
    //         // log.info("Found primary " + strData);
    //         String[] primary = strData.split(":");
    //         String primaryHost = primary[0];
    //         int primaryPort = Integer.parseInt(primary[1]);

    //         Boolean isPrimary = false;

    //         if (primaryHost.equals(host) && primaryPort == port) {
    //             isPrimary = true;
    //         }

    //         // log.info("Is Primary: " + isPrimary);

    //         return isPrimary;
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //         throw e;
    //     }
    // }

    // public ConcurrentLinkedQueue<KeyValueService.Client> createBackupClients(int clientNumber) throws Exception {
    //     try {
    //         if (!isPrimary) {
    //             // System.out.println("This is Backup. No need to create backup client.");
    //             return null;
    //         }

    //         curClient.sync();
    //         List<String> childrenKeys = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    
    //         if (childrenKeys.size() <= 1) {
    //             // log.info("This is primary. But there is no Backup found");
    //             return null;
    //         }
    
    //         Collections.sort(childrenKeys);
    //         byte[] data;

    //         if (childrenKeys.size() > 2) {
    //             System.out.println("There are more than 2 nodes.");
    //             data = curClient.getData().forPath(zkNode + "/" + childrenKeys.get(childrenKeys.size() - 1));
    //         } else {
    //             data = curClient.getData().forPath(zkNode + "/" + childrenKeys.get(1));
    //         }

    //         String strData = new String(data);
    //         String[] backupData = strData.split(":");
    //         String backupHost = backupData[0];
    //         int backupPort = Integer.parseInt(backupData[1]);
    
    //         // log.info("This is primary. Found backup " + strData);
    //         // log.info("Setting up " + clientNumber + " backup clients.");

    //         ConcurrentLinkedQueue<KeyValueService.Client> backupClients = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
    //         for(int i = 0; i < clientNumber; i++) {
    //             TSocket sock = new TSocket(backupHost, backupPort);
    //             TTransport transport = new TFramedTransport(sock);
    //             transport.open();
    //             TProtocol protocol = new TBinaryProtocol(transport);
        
    //             backupClients.add(new KeyValueService.Client(protocol));
    //         }
    
    //         return backupClients;
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //         throw e;
    //     }
    // }

    public void copyData(Map<String, String> data) throws org.apache.thrift.TException {
        // Lock the entire hashmap on backup
        // lock.lock();

        try {
            this.myMap = new ConcurrentHashMap<String, String>(data); 
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            // lock.unlock();
        }
    }

	synchronized public void process(WatchedEvent event) {
        // log.info("ZooKeeper event " + event);
        // Lock the entire hashmap on primary
        try {
            // Get all the children
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

            if (children.size() == 0) {
                // log.error("No children found");
                this.isPrimary = false;
                return;
            }

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
                // System.out.println("Does have backup clients.");
                String strBackupData = new String(backupData);
                String[] backup = strBackupData.split(":");
                String backupHost = backup[0];
                int backupPort = Integer.parseInt(backup[1]);

                // // Copy data to backup
                // TSocket sock = new TSocket(backupHost, backupPort);
                // TTransport transport = new TFramedTransport(sock);
                // transport.open();
                // TProtocol protocol = new TBinaryProtocol(transport);
                // KeyValueService.Client backupClient = new KeyValueService.Client(protocol);

                lock.lock();

                // backupClient.copyData(this.myMap);
                // System.out.println("Copy Data to backup Succeeded!");

                // Create 32 backup clients
                this.backupClients = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
                for(int i = 0; i < clientNumber; i++) {
                    TSocket sock = new TSocket(backupHost, backupPort);
                    TTransport transport = new TFramedTransport(sock);
                    transport.open();
                    TProtocol protocol = new TBinaryProtocol(transport);
            
                    this.backupClients.add(new KeyValueService.Client(protocol));
                }

                lock.unlock();

                System.out.println("BackupClients: " + this.backupClients);
                System.out.println(this.backupClients.size() + " backup clients are created.");
            } else {
                // System.out.println("Does not have backup clients.");
                this.backupClients = null;
            }
        } catch (Exception e) {
            // log.error("Unable to determine primary or children");
        }
    }
}
