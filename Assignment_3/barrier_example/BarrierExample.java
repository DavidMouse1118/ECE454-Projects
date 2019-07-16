import java.io.*;
import java.util.*;
import java.lang.Thread;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

import org.apache.log4j.*;

public class BarrierExample {

	public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();

		if (args.length != 2) {
			System.err.println("Usage: java BarrierExample zkconnectstring barrier_node");
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

		DistributedBarrier barrier = new DistributedBarrier(curClient, args[1]+"_barrier");
		barrier.setBarrier();

		// The additional thread here simulates another machine inside the cluster. 
		Thread thread = new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(3000);
					System.out.println("Removing barrier in 5 seconds...");
					Thread.sleep(1000);
					System.out.println("Removing barrier in 4 seconds...");
					Thread.sleep(1000);
					System.out.println("Removing barrier in 3 seconds...");
					Thread.sleep(1000);
					System.out.println("Removing barrier in 2 seconds...");
					Thread.sleep(1000);
					System.out.println("Removing barrier in 1 second...");
					Thread.sleep(1000);					
					barrier.removeBarrier();
					System.out.println("Barrier node has been removed");
				} catch (InterruptedException e) {
					System.out.println("Program terminated by ctrl-c");
				} catch (Exception e) {
					System.out.println("removeBarrier returned error: " + e.getMessage());
				}
			}
		});  
		thread.start();

		System.out.println("====== Program is blocked by the barrier ======");
		barrier.waitOnBarrier();
		System.out.println("====== Program resumes execution ======");

	}
}
