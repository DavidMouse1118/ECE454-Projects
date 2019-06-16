import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mindrot.jbcrypt.BCrypt;

public class BEHandler implements BcryptService.Iface {
	public List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
        System.out.println("Backend gets the hashPassword request");
        long startTime = System.currentTimeMillis();
        
		try {
            List<String> ret = new ArrayList<>();

			for (String ps : password) {
				
				String oneHash = BCrypt.hashpw(ps, BCrypt.gensalt(logRounds));
				ret.add(oneHash);
				System.out.println(oneHash);
            }
            
            System.out.println("Time elapsed for hash passward: " + (System.currentTimeMillis() - startTime) + " ms");

			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash)
	throws IllegalArgument, org.apache.thrift.TException {
        System.out.println("Backend gets the checkPassword request");
        long startTime = System.currentTimeMillis();

		try {
			List<Boolean> ret = new ArrayList<>();

			for (int i = 0; i < password.size(); i++) {
				String onePwd = password.get(i);
				String oneHash = hash.get(i);
				try {
					ret.add(BCrypt.checkpw(onePwd, oneHash));
				} catch (Exception e) {
					ret.add(false);
				}
            }
            
            System.out.println("Time elapsed for checkPassword: " + (System.currentTimeMillis() - startTime) + " ms");

			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	@Override
	public void beToFeRegistrar(String beHost, int bePort) {
		throw new UnsupportedOperationException();
	}
}
