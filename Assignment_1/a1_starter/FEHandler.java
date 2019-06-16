import java.lang.reflect.Constructor;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransportException;

import org.mindrot.jbcrypt.BCrypt;

public class FEHandler implements BcryptService.Iface {
	class BENode {
		String beHost = "";
		int bePort = 0;
	
		public BENode(String beHost, int bePort){
			this.beHost = beHost;
			this.bePort = bePort;
		}
	}

	public int taskNum = 0;
	static Semaphore semaphore = new Semaphore(100);
	// public List<BENode> liveBE = new ArrayList<>();
	public List<BENode> liveBE = new CopyOnWriteArrayList<>();

	public void beToFeRegistrar(String beHost, int bePort) {
		try {
			semaphore.acquire();
			liveBE.add(new BENode(beHost, bePort));
			
			System.out.println("Backend server " + beHost + ":" + bePort + " is alive!");
			
			System.out.println("There are " + liveBE.size() + " backend server are running.");
			semaphore.release();
		// Thread.sleep(1000);
		} catch (Exception e) {
			//TODO: handle exception
		}
		
	}
	
	@Override
	public List<String> hashPassword(List<String> password, short logRounds)
			throws IllegalArgument, org.apache.thrift.TException {
				try {
					if (password == null || password.size() == 0) {
						throw new IllegalArgumentException("Invalid password list input");
					}

					if (logRounds > 30 || logRounds < 4) {
						throw new IllegalArgumentException("Invalid logRound");
					}

					
	
					// Offload to backend
					List<String> hash = new ArrayList<String>();
					boolean offloadSuccess = false;
					// semaphore.acquire();
					while (!offloadSuccess) {
						if (liveBE.size() == 0) {
							System.out.println("There is not BE node. Hash Passward in FE");
							List<String> ret = new ArrayList<>();
		
							for (String ps : password) {
								String oneHash = BCrypt.hashpw(ps, BCrypt.gensalt(logRounds));
								ret.add(oneHash);
							}
							offloadSuccess = true;
							return ret;
						}

						BENode server = getBEServer();
						TSocket sock = new TSocket(server.beHost, server.bePort);
						TTransport transport = new TFramedTransport(sock);
						TProtocol protocol = new TBinaryProtocol(transport);
						BcryptService.Client client = new BcryptService.Client(protocol);
						System.out.println("Offload to backend " + server.beHost + ":" + server.bePort + " for hash password.");
						// Thread.sleep(1000);
						try {
							transport.open();
							hash = client.hashPassword(password, logRounds);
							transport.close();
							offloadSuccess = true;
						} catch (TTransportException t) {
							System.out.println("Failed connect to target BE, drop it.");
							liveBE.remove(server);
							System.out.println(t);
						}
					}
					// semaphore.release();		
					return hash;
				} catch (Exception e) {
					throw new IllegalArgument(e.getMessage());
				}
			}
	
	@Override
	public List<Boolean> checkPassword(List<String> password, List<String> hash)
			throws IllegalArgument, org.apache.thrift.TException {
				try {
					if (password == null || password.size() == 0) {
						throw new IllegalArgumentException("Invalid password list input");
					}

					if (password.size() != hash.size()) {
						throw new IllegalArgumentException("The password and hash arguments of checkPassword are lists of unequal length");
					}

					
	
					// Offload to backend
					List<Boolean> result = new ArrayList<Boolean>();
					boolean offloadSuccess = false;
					// semaphore.acquire();
					while (!offloadSuccess) {
						if (liveBE.size() == 0) {
							System.out.println("There is not BE node. Hash Passward in FE");
							List<Boolean> ret = new ArrayList<>();
					
							for (int i = 0; i < password.size(); i++) {
								String onePwd = password.get(i);
								String oneHash = hash.get(i);
				
								ret.add(BCrypt.checkpw(onePwd, oneHash));
							}
							offloadSuccess = true;
							return ret;
						}

						BENode server = getBEServer();
						TSocket sock = new TSocket(server.beHost, server.bePort);
						TTransport transport = new TFramedTransport(sock);
						TProtocol protocol = new TBinaryProtocol(transport);
						BcryptService.Client client = new BcryptService.Client(protocol);
						System.out.println("Offload to backend " + server.beHost + ":" + server.bePort + " for check password.");
						// Thread.sleep(1000);
						try {
							transport.open();
							// System.out.println("should not print !!!!!!!!!!!");
							result = client.checkPassword(password, hash);
							transport.close();
							offloadSuccess = true;
						} catch (TTransportException t) {
							System.out.println("Failed connect to target BE, drop it.");
							liveBE.remove(server);
							System.out.println(t);
						}
					}
					// semaphore.release();			
					return result;					
				} catch (Exception e) {
					throw new IllegalArgument(e.getMessage());
				}
			}


	public BENode getBEServer() throws Exception {
		// int index = ThreadLocalRandom.current().nextInt(0, liveBE.size());
		try {
			semaphore.acquire();
			int index = taskNum % liveBE.size();
			taskNum++;
			System.out.println("liveBE size: " + liveBE.size());
			System.out.println("We choose the # " + (index + 1) + " Backend server");
			semaphore.release();
			return liveBE.get(index);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}	
	}
}
