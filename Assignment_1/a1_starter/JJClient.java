import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class JJClient {

    private static Logger log = Logger.getLogger(JJClient.class.getName());
    private static final String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

    private static class StartNewRPCTask implements Runnable {

        private Thread t;
        private String threadName;
        private String hostName;
        private int portNumber;
        private int batchSize1;
        private int batchSize2;
        private int pwdLen;
        private short logRounds;
        CountDownLatch latch;

        StartNewRPCTask(String threadName, String hostName, int portNumber, int batchSize1, int batchSize2, int pwdLen, short logRounds,
                        CountDownLatch latch) {
            this.threadName = threadName;
            this.hostName = hostName;
            this.portNumber = portNumber;
            this.batchSize1 = batchSize1;
            this.batchSize2 = batchSize2;
            this.pwdLen = pwdLen;
            this.logRounds = logRounds;
            this.latch = latch;
        }

        public void run() {
            try {
                TSocket sock = new TSocket(hostName, portNumber);
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client client = new BcryptService.Client(protocol);
                transport.open();

                List<String> pwdList1 = generateRandomPasswordList(batchSize1, pwdLen);
                List<String> pwdList2 = generateRandomPasswordList(batchSize2, pwdLen);
                log.info("generate pwdList1:" + pwdList1);
                log.info("generate pwdList2:" + pwdList2);

                List<String> hash1 = client.hashPassword(pwdList1, logRounds);
                List<String> hash2 = client.hashPassword(pwdList2, logRounds);

                List<Boolean> check1 = client.checkPassword(pwdList1, hash1);
                List<Boolean> check2 = client.checkPassword(pwdList2, hash2);
                List<Boolean> negativeCheck = client.checkPassword(pwdList1, hash2);
                List<Boolean> exceptionCheck = client.checkPassword(pwdList1, pwdList2);

                boolean sumaryCheck1 = true;
                boolean sumaryCheck2 = true;
                boolean sumaryNegativeCheck = true;
                boolean sumaryExceptionCheck = true;
                for (int i = 0; i < batchSize1; i++) {
                    sumaryCheck1 = sumaryCheck1 && check1.get(i);
                    sumaryCheck2 = sumaryCheck2 && check2.get(i);
                    sumaryNegativeCheck = sumaryNegativeCheck && !negativeCheck.get(i);
                    sumaryExceptionCheck = sumaryExceptionCheck && !exceptionCheck.get(i);
                }

                String sumary = String.format("Summary: positive check1 [%s]; positive check2 [%s]; negative check [%s]; exception check [%s]", sumaryCheck1, sumaryCheck2, sumaryNegativeCheck, sumaryExceptionCheck);

                log.info(sumary);

                String logFile = "tmp/" + threadName + ".log";
                File file = new File(logFile);
                if (file.exists()) file.delete();
                try {
                    if (!file.exists()) file.createNewFile();
                    Files.write(Paths.get(logFile), hash1.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), hash2.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), check1.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), check2.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), negativeCheck.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), exceptionCheck.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), sumary.getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get(logFile), "\n".getBytes(), StandardOpenOption.APPEND);
                } catch (Exception e) {
                    e.printStackTrace();
                }

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
            System.err.println("Usage: java JJClient FE_host FE_port batchSize1 batchSize2 pwdLen logRound threadNum");
            System.exit(-1);
        }

        BasicConfigurator.configure();

        String hostName = args[0];
        int portNumber = Integer.parseInt(args[1]);
        int batchSize1 = Integer.parseInt(args[2]);
        int batchSize2 = Integer.parseInt(args[3]);
        int pwdLen = Integer.parseInt(args[4]);
        short logRound = (short) Integer.parseInt(args[5]);
        int threadNumber = Integer.parseInt(args[6]);
        CountDownLatch latch = new CountDownLatch(threadNumber);

        for (int i = 0; i < threadNumber; i++) {
            JJClient.StartNewRPCTask task = new JJClient.StartNewRPCTask("client_" + i, hostName, portNumber, batchSize1, batchSize2, pwdLen, logRound, latch);
            task.start();
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static List<String> generateRandomPasswordList(int size, int length) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(randomStr(length));
        }
        return result;
    }

    private static String randomStr(int num) {
        if (num == 0) return "";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        int bound = SALTCHARS.length();
        for (int i = 0; i < num; i++) {
            salt.append(SALTCHARS.charAt(rnd.nextInt(bound)));
        }
        return salt.toString();
    }
}