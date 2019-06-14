import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.mindrot.jbcrypt.BCrypt;

public class FEHandler implements BcryptService.Iface {
	class BENode {
		String beHost = "";
		int bePort = 0;

		public BENode(String beHost, int bePort) {
			this.beHost = beHost;
			this.bePort = bePort;
		}
	}

	public List<BENode> liveBE = new ArrayList<>();
	public ConcurrentHashMap<BENode, BcryptService.Client> BENodeService = new ConcurrentHashMap<>();
	public Integer circularBEPointer = 0;

	public void beToFeRegistrar(String beHost, int bePort) {
		liveBE.add(new BENode(beHost, bePort));

		System.out.println("Backend server " + beHost + ":" + bePort + " is alive!");

		System.out.println("There are " + liveBE.size() + " backend server are running.");
	}

	@Override
	public List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
		try {
			if (liveBE.size() == 0) {
				System.out.println("There is not BE node. Hash Passward in FE");
				List<String> ret = new ArrayList<>();

				for (String ps : password) {
					String oneHash = BCrypt.hashpw(ps, BCrypt.gensalt(logRounds));
					ret.add(oneHash);
				}

				return ret;
			}

			// Offload to backend
			System.out.println("Offload to backend for chech password.");
			BENode server = getBEServer();
			BcryptService.Client client = getBEServerClient(server);
			List<String> hash = client.hashPassword(password, logRounds);

			return hash;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	@Override
	public List<Boolean> checkPassword(List<String> password, List<String> hash)
			throws IllegalArgument, org.apache.thrift.TException {
		try {
			if (liveBE.size() == 0) {
				System.out.println("There is not BE node. Hash Passward in FE");
				List<Boolean> ret = new ArrayList<>();

				for (int i = 0; i < password.size(); i++) {
					String onePwd = password.get(i);
					String oneHash = hash.get(i);

					ret.add(BCrypt.checkpw(onePwd, oneHash));
				}

				return ret;
			}

			// Offload to backend
			System.out.println("Offload to backend for chech password.");
			BENode server = getBEServer();
			BcryptService.Client client = getBEServerClient(server);
			System.out.println("start");
			List<Boolean> result = client.checkPassword(password, hash);
			System.out.println("end");

			return result;
		} catch (Exception e) {
			return null;
			// throw new IllegalArgument(e.getMessage());
		}
	}

	public BENode getBEServer() {
		circularBEPointer = circularBEPointer % liveBE.size();

		System.out.println("We choose the # " + (circularBEPointer + 1) + " Backend server");

		return liveBE.get(circularBEPointer++);
	}

	public BcryptService.Client getBEServerClient(BENode server) throws IllegalArgument, org.apache.thrift.TException {
		try {
			if (BENodeService.containsKey(server)) {
				System.out.println("Find existing connection");
				return BENodeService.get(server);
			}

			System.out.println("Create new connection...");
			TSocket sock = new TSocket(server.beHost, server.bePort);
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			// Add it to hash
			BENodeService.put(server, client);
			transport.open();

			return client;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}
}
