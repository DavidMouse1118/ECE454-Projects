import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MultiThreadClient {
    static int counter = 10;
    static CountDownLatch latch = new CountDownLatch(counter);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        if (args.length != 3) {
            System.err.println("Usage: java Client FE_host FE_port password");
            System.exit(-1);
        }

        try {
            // number of Client nodes
            for (int i = 0; i < 16; ++i) {
                System.out.println("Send request i = " + i);
                new Thread() {
                    public void run() {
                        try {
                            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
                            TTransport transport = new TFramedTransport(sock);
                            TProtocol protocol = new TBinaryProtocol(transport);
                            BcryptService.Client client = new BcryptService.Client(protocol);
                            transport.open();
    
                            List<String> password = new ArrayList<>();
                            for (int i = 0; i < 10; i++) {
                                password.add(getAlphaNumericString(1024));
                            }
                            List<String> hash = client.hashPassword(password, (short) 10);
                            System.out.println("Password: " + password.get(0));
                            System.out.println("Hash: " + hash.get(0));
                            System.out.println("Positive check: " + client.checkPassword(password, hash));
                            hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
                            System.out.println("Negative check: " + client.checkPassword(password, hash));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
                System.out.println("After Send request i = " + i);
            }
    
            // // boolean wait = latch.await(30, TimeUnit.SECONDS);
            // System.out.println("latch.await =:" + wait);
            System.out.println("Exiting client.");
            System.out.println("Time to send all request: " + (System.currentTimeMillis() - startTime) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static String getAlphaNumericString(int n) {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789" + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index = (int) (AlphaNumericString.length() * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString.charAt(index));
        }

        return sb.toString();
    }
}
