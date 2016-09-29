import java.util.*;
import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class KeyValueHandler implements KeyValueService.Iface {
	private HashMap<String, String> storage = new HashMap<String, String>();
	private final HashMap<Integer, String> hosts;
	private final HashMap<Integer, Integer> ports;
    private final int myNum, serverNum;
	//private List<LinkedList<String>> keyPar = new ArrayList<LinkedList<String>>(serverNum);
	//private List<LinkedList<Integer>> posIdx = new  ArrayList<LinkedList<Integer>>(serverNum);
	public KeyValueHandler(int myNum, HashMap<Integer, String> hosts,HashMap<Integer, Integer> ports){
		this.myNum = myNum;
		this.hosts = hosts;
		this.ports = ports;
		this.serverNum = hosts.size();
	}
	
	public Object[] partition(List<String> keys){
		List<LinkedList<String>> keyPar = new ArrayList<LinkedList<String>>(serverNum);
		//List<LinkedList<Integer>> posIdx = new  ArrayList<LinkedList<Integer>>(serverNum);
		int[] posIdx = new int[keys.size()];
		System.out.println(serverNum + " Storage Nodes");
		for(int i = 0; i < serverNum; i++){
			keyPar.add(i, new LinkedList<>());
			//posIdx.add(i, new LinkedList<>());
		}
		int idx = 0;
		for(String key : keys){
			int snNum = key.hashCode() % serverNum;
			keyPar.get(snNum).add(key);
			//posIdx.get(snNum).add(idx);
			posIdx[idx] = snNum;
			idx++;
		}
		Object[] ret = new Object[2];
		ret[0] = keyPar;
		ret[1] = posIdx;
		return ret;
	}
	
	public void multiPut(List<String> keys, List<String> values) throws IllegalArgument{
		if(keys.size() != values.size()){
			throw new IllegalArgument("size not equal!");
		}
		
		System.out.println("MultiPut Partitioning...");
		Object[] pat = partition(keys);
		ArrayList<LinkedList<String>> keyPar = (ArrayList<LinkedList<String>>) pat[0];
		//ArrayList<LinkedList<Integer>> posIdx = (ArrayList<LinkedList<Integer>>) pat[1];
		int[] posIdx = (int[]) pat[1];
		List<LinkedList<String>> valPar = new ArrayList<LinkedList<String>>(serverNum);
		for(int i = 0; i < serverNum; i++){
			valPar.add(i, new LinkedList<>());
		}
		
		for(int id = 0; id < posIdx.length; id++){
			System.out.println("id:" + id + ", " + posIdx[id] + ", " + values.get(id));
			valPar.get(posIdx[id]).add(values.get(id));
		}
		
		for(int sn = 0; sn < serverNum; sn++){
			
			if(sn == myNum){//storageNode is current server
				//System.out.println("Store in Local Server...");
				System.out.println("Store in Server: " + hosts.get(sn) + " " + ports.get(sn) + ":");
				for(int j = 0; j < keyPar.get(sn).size(); j++){
					String key = keyPar.get(sn).get(j);
					String val = valPar.get(sn).get(j);
					System.out.println(key + ", " + val);
					storage.put(key, val);
				}
			}else{//storageNode is NOT current server
				if(keyPar.get(sn).size() > 0){
					System.out.println("Store in Server: " + hosts.get(sn) + " " + ports.get(sn) + ":");
					try{
					//System.out.println("Store in Remote Server: " + hosts.get(sn) + " " + ports.get(sn));
					TSocket sock = new TSocket(hosts.get(sn), ports.get(sn));
			  		TTransport transport = new TFramedTransport(sock);
			  		TProtocol protocol = new TBinaryProtocol(transport);
			  		KeyValueService.Client client = new KeyValueService.Client(protocol);
			  		transport.open();
			  		client.multiPut(keyPar.get(sn), valPar.get(sn));
			  		transport.close();
					}catch(TException x){
						x.printStackTrace();
					}
				}
				
			}
		}
	}
	
	public List<String> multiGet(List<String> keys){
		System.out.println("multiGet Partitioning...");
		Object[] pat = partition(keys);
		ArrayList<LinkedList<String>> keyPar = (ArrayList<LinkedList<String>>) pat[0];
		//ArrayList<LinkedList<Integer>> posIdx = (ArrayList<LinkedList<Integer>>) pat[1];
		int[] posIdx = (int[]) pat[1];
		List<LinkedList<String>> getVal = new ArrayList<LinkedList<String>>(serverNum);
		
		for(int i = 0; i < serverNum; i++){
			getVal.add(i, new LinkedList<>());
		}
		
		for(int sn = 0; sn < serverNum; sn++){
			LinkedList<String> retVal = new LinkedList<>();
			if(sn == myNum){
				System.out.println("Get from Server: " + hosts.get(sn) + " " + ports.get(sn) + ":");
				for(String key : keyPar.get(sn)){
					if(storage.containsKey(key)){
						System.out.println(key + ", " + "storage.get(key)");
						retVal.add(storage.get(key));
					}	
					else
						retVal.add("");
				}
				getVal.add(sn, retVal);
			}else{
				if(keyPar.get(sn).size() > 0){
					System.out.println("Get from Server: " + hosts.get(sn) + " " + ports.get(sn) + ":");
					retVal = remoteGet(keyPar.get(sn), hosts.get(sn), ports.get(sn));
					
					getVal.add(sn, retVal);
				}
			}
		}
		List<String> ret = new ArrayList<String>();
		for(int id = 0; id < posIdx.length; id++){
			int snNum = posIdx[id];
			ret.add(getVal.get(snNum).removeFirst());
		}
		return ret;
	}
	private LinkedList<String> remoteGet(List<String> keys, String remoteHost, int remotePort){
		
		LinkedList<String> ret = new LinkedList<String>();
		try{
			System.out.println("Get Value from Remote Server: " + remoteHost + " " + remotePort);
			TSocket sock = new TSocket(remoteHost, remotePort);
	  		TTransport transport = new TFramedTransport(sock);
	  		TProtocol protocol = new TBinaryProtocol(transport);
	  		KeyValueService.Client client = new KeyValueService.Client(protocol);
	  		List<String> getRemote = new LinkedList<String>();
	  		transport.open();
	  		getRemote = client.multiGet(keys);
	  		transport.close();
	  		for(String s : getRemote){
	  			ret.add(s);
	  		}
	  		//getVal.add(sn, getRemote);
		}catch (TException x) {
	    	x.printStackTrace();
	    }
		return ret;
	}
	
}
	
