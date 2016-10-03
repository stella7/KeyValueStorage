import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class KeyValueHandler implements KeyValueService.Iface {
	private HashMap<String, String> storage = new HashMap<String, String>();
	private final HashMap<Integer, String> hosts;
	private final HashMap<Integer, Integer> ports;
    private final int myNum, serverNum;
    private HashMap<Integer,LinkedList<KeyValueService.Client>> clientMap;

	public KeyValueHandler(int myNum, HashMap<Integer, String> hosts,HashMap<Integer, Integer> ports){
		this.myNum = myNum;
		this.hosts = hosts;
		this.ports = ports;
		this.serverNum = hosts.size();
		this.clientMap = new HashMap<Integer, LinkedList<KeyValueService.Client>>();
	}
	
	public void addConnection(Integer nodeKey, LinkedList<KeyValueService.Client> clients){
		clientMap.put(nodeKey, clients);
	}
	
	public void multiPut(List<String> keys, List<String> values) throws IllegalArgument{
		//long start = System.nanoTime();
		if(keys.size() != values.size()){
			throw new IllegalArgument("size not equal!");
		}
		
		//System.out.println("MultiPut Partitioning...");
		ArrayList<String>[] keyPar = new ArrayList[serverNum];
		ArrayList<String>[] valPar = new ArrayList[serverNum];

		//System.out.println(serverNum + " Storage Nodes");
		for(int i = 0; i < serverNum; i++){
			keyPar[i] = new ArrayList<>();
			valPar[i] = new ArrayList<>();
			//posIdx.add(i, new LinkedList<>());
		}
		int idx = 0;
		for(int i = 0; i < keys.size(); i++){
			String key = keys.get(i);
			int snNum = (key.hashCode() & 0x7FFFFFFF) % serverNum;
			//System.out.println(snNum + ", " +key);
			keyPar[snNum].add(key);
			String val = values.get(i);
			valPar[snNum].add(val);
			//posIdx.get(snNum).add(idx);
			//posIdx[idx] = snNum;
			idx++;
		}
		//long end = System.nanoTime() - start;
		for(Integer sn = 0; sn < serverNum; sn++){
			
			if(sn == myNum){//storageNode is current server
				//System.out.println("Store in Server: " + hosts.get(sn) + " " + ports.get(sn) + ":");
				for(int j = 0; j < keyPar[sn].size(); j++){
					String key = keyPar[sn].get(j);
					String val = valPar[sn].get(j);
					//System.out.println(key + ", " + val);
					storage.put(key, val);
				}
			}else{//storageNode is NOT current server
				//long openStart = System.nanoTime();
				if(keyPar[sn].size() > 0){
					try{
						KeyValueService.Client client = FetchClient(sn);
				  		client.multiPut(keyPar[sn], valPar[sn]);
				  		establishClient(sn,client);
				  		//transport.close();
					}catch(Exception x){
						x.printStackTrace();
					}
					//long openEnd = System.nanoTime() - openStart;
					//System.out.println("open " + openEnd);
				}
				
			}
		}
		//long end3 = System.nanoTime() - start;
		//System.out.println(end + " " + end3);
		
	}
	
	public List<String> multiGet(List<String> keys){
		//System.out.println("multiGet Partitioning...");
		List<LinkedList<String>> keyPar = new ArrayList<LinkedList<String>>(serverNum);
		int[] posIdx = new int[keys.size()];
		//System.out.println(serverNum + " Storage Nodes");
		for(int i = 0; i < serverNum; i++){
			keyPar.add(i, new LinkedList<>());
			//posIdx.add(i, new LinkedList<>());
		}
		int idx = 0;
		for(String key : keys){
			int snNum = (key.hashCode() & 0x7FFFFFFF) % serverNum;
			//System.out.println(snNum + ", " +key);
			keyPar.get(snNum).add(key);
			//posIdx.get(snNum).add(idx);
			posIdx[idx] = snNum;
			idx++;
		}
		
		List<ConcurrentLinkedQueue<String>> getVal = new ArrayList<ConcurrentLinkedQueue<String>>(serverNum);
		
		for(int i = 0; i < serverNum; i++){
			getVal.add(i, new ConcurrentLinkedQueue<>());;
		}
		
		for(Integer sn = 0; sn < serverNum; sn++){
			ConcurrentLinkedQueue<String> retVal = new ConcurrentLinkedQueue<>();
			if(sn == myNum){
				//System.out.println("Get from Server: " + hosts.get(sn) + " " + ports.get(sn) + ":");
				//System.out.println(keyPar.get(sn).size());
				for(String key : keyPar.get(sn)){
					//System.out.println("Get key: " + key);
					if(storage.containsKey(key)){
						//System.out.println(key + ", " + storage.get(key));
						retVal.add(storage.get(key));
					}	
					else
						retVal.add("");
				}
				getVal.add(sn, retVal);
			}else{
				if(keyPar.get(sn).size() > 0){
					retVal = remoteGet(keyPar.get(sn), sn);
					getVal.add(sn, retVal);
				}
			}
		}
		List<String> ret = new ArrayList<String>();
		for(int id = 0; id < posIdx.length; id++){
			int snNum = posIdx[id];
			ret.add(getVal.get(snNum).poll());
		}
		return ret;
	}
	
	private ConcurrentLinkedQueue<String> remoteGet(List<String> keys, Integer sn){
		
		ConcurrentLinkedQueue<String> ret = new ConcurrentLinkedQueue<String>();
		try{

			KeyValueService.Client client = FetchClient(sn);
	  		List<String> getRemote = new LinkedList<String>();
	  		getRemote = client.multiGet(keys);

	  		establishClient(sn,client);
	  		for(String s : getRemote){
	  			ret.add(s);
	  		}
		}catch (TException x) {
	    	x.printStackTrace();
	    }
		return ret;
	}
	
	private void establishClient(Integer nodeKey, KeyValueService.Client client){
		LinkedList<KeyValueService.Client> clients = clientMap.get(nodeKey);
   		synchronized(clients) {
	    	clients.add(client);
    	}
    }

    private KeyValueService.Client FetchClient(Integer nodeKey){
    	if (nodeKey==myNum){
    		return null;
    	}
    	LinkedList<KeyValueService.Client> clients = clientMap.get(nodeKey);
    	synchronized(clients) {
			KeyValueService.Client client = clients.removeFirst();
			return client;
		}
    		
  	}
	
}
	
