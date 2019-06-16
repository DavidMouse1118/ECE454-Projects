import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.*;

public class YJClient {
    static Logger log = Logger.getLogger(BENode.class.getName());
    static long totalTime = 0;

    private static class StartNewRPCTask implements Runnable {

        private Thread t;
        private String threadName;
        private String hostName;
        private short portNumber;
        private int batchSize;
        private short logRounds;
        private String word;
        CountDownLatch latch;


        StartNewRPCTask(String threadName, String hostName, short portNumber, int batchSize, short logRounds,
                        String word, CountDownLatch latch) {
            this.threadName = threadName;
            this.hostName = hostName;
            this.portNumber = portNumber;
            this.batchSize = batchSize;
            this.logRounds = logRounds;
            this.word = word;
            this.latch = latch;
        }

        public void run() {
            try {
                long threadStartTime = System.currentTimeMillis();
                TSocket sock = new TSocket(hostName, portNumber);
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client client = new BcryptService.Client(protocol);
                transport.open();
                log.info("connect to FE success");

                List<String> password = new ArrayList<>();
                for (int i = 0; i < batchSize; i++)
                    password.add(word);

                //System.out.println("Password: " + password);

                long start = System.currentTimeMillis();
                List<String> hash = client.hashPassword(password, logRounds);
                //System.out.println("Hash: " + hash);
                System.out.println("elapsed hashing time: " + (System.currentTimeMillis() - start));
                //System.out.println("---");

                start = System.currentTimeMillis();
                client.checkPassword(password, hash);
                //System.out.println("Positive check: " + client.checkPassword(password, hash));
                System.out.println("elapsed checking time: " + (System.currentTimeMillis() - start));
                //System.out.println("---");

				/*
                start = System.currentTimeMillis();
				hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
				System.out.println("Negative check: " + client.checkPassword(password, hash));
				System.out.println("elapsed time: " + (System.currentTimeMillis() - start));
				System.out.println("---");


				start = System.currentTimeMillis();
				hash.set(0, "too short");
				System.out.println("Exception check: " + client.checkPassword(password, hash));
				System.out.println("elapsed time: " + (System.currentTimeMillis() - start));
				System.out.println("---");
				*/
                long eclapsedTime = System.currentTimeMillis() - threadStartTime;
                totalTime += eclapsedTime;
                System.out.println("total processing time for single thread " + eclapsedTime);

                transport.close();
            } catch (TException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }

        public void start() {
            if (t == null) {
                t = new Thread(this, threadName);
                t.start();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 7) {
            System.err.println("Usage: java YJClient FE_host FE_port password pwdlen size logRound threadNum");
            System.exit(-1);
        }
        String hostName = args[0];
        short portNumber = (short) Integer.parseInt(args[1]);
        String password = args[2];
        int pwdlen = Integer.parseInt(args[3]);
        while (password.length() < pwdlen) password += args[2];
        int size = Integer.parseInt(args[4]);
        short logRound = (short) Integer.parseInt(args[5]);
        int threadNumber = Integer.parseInt(args[6]);
        CountDownLatch latch = new CountDownLatch(threadNumber);
        long clientStartTime = System.currentTimeMillis();
        for (int i = 0; i < threadNumber; i++) {
            StartNewRPCTask task = new StartNewRPCTask("client task", hostName, portNumber, size, logRound, password, latch);
            task.start();
        }
        try {
            latch.await();
            System.out.println(String.format("size %d round %d thread %d", size, logRound, threadNumber));
            System.out.println("average processing time for each thread: " + (1.0 * totalTime / threadNumber));
            int time = (int) (System.currentTimeMillis() - clientStartTime);
            System.out.println("client processing time " + time);
            String result = String.format("word_length %d size %d round %d thread %d: %d \n", pwdlen, size, logRound, threadNumber, time);
            String logFile = "result.log";
            File file = new File(logFile);
            if (!file.exists()) file.createNewFile();
            try {
                Files.write(Paths.get("result.log"), result.getBytes(), StandardOpenOption.APPEND);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
