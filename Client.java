import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.security.SecureRandom;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;


import org.apache.thrift.protocol.*;
import org.apache.log4j.*;

public class Client {
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static SecureRandom rnd = new SecureRandom();
	static HashMap<String, String> keyVal = new HashMap<String, String>();
	static int listLen = 10, keyLen = 10, valLen = 10;
	static Logger log;
	public static void main(String [] args) {
		
		  	try {
		  		BasicConfigurator.configure();
		        log = Logger.getLogger(Client.class.getName());
		  		
		  		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		  		HashMap<Integer, String> hosts = new HashMap<Integer, String>();
		  		HashMap<Integer, Integer> ports  = new HashMap<Integer, Integer>();
		  		String line;
		  		int i = 0;
		  		while ((line = br.readLine()) != null) {
			  		String[] parts = line.split(" ");
			  		hosts.put(i, parts[0]);
			  		ports.put(i, Integer.parseInt(parts[1]));
			  		i++;
		  		}

		  		System.out.println(hosts.get(0) + ports.get(0));

		  		Test(hosts, ports);     
		  		//System.out.println("It works");
		  		
		  		
		  	}

		  	catch (Exception x) {
		  		x.printStackTrace();
		  	}
	}

  public static void Test(HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports){

    try {
    	
    	/*Genrate keylist for multiGet*/
    	
    	/*
    	for(String k : getKey){
    		System.out.println("Get: " + k);
    	}
    	*/
    	//System.out.println("Finish Generating KeyValue");
        /*------------------------*/
  		int threadNum = 32;
        Thread[] threads = new Thread[threadNum];
        for(int j = 0; j < threadNum; j++){
      	  threads[j] = new Thread(new clientThread(j, hosts.get(0), ports.get(0)));
      	  threads[j].start();
        }
        
		for (int t = 0; t < threadNum; t++) {
		    threads[t].join();
		}
        
     }catch (Exception x){
    	 x.printStackTrace();
     }
    
  }
  
  static KeyValueService.Client startClient(String host, Integer port){
	  while(true){

	  	  try{
			  TSocket sock = new TSocket(host, port);
			  TTransport transport = new TFramedTransport(sock);
			  TProtocol protocol = new TBinaryProtocol(transport);
			  KeyValueService.Client client = new KeyValueService.Client(protocol);
			  transport.open();
			  return client;
		  }catch(Exception e){
			  log.error("Unable to connect to server");
		  }
	  	
	  }

	  
  }
  
  private static boolean compare(HashMap<String, String> keyVal, List<String> keys, List<String> retList){
	  boolean ret = true;
	  for(int i = 0; i < keys.size(); i++){
		  String key = keys.get(i);
		  
		  String retVal = retList.get(i);
		  String val;
		  if(keyVal.containsKey(key))
			  val = keyVal.get(key);
		  else val = "";
		  //System.out.println("search: " + key + ", " + val);
		  if(!val.equals(retVal)){
			  ret = false;
			  System.out.println("multiGet for key: " + key + " received " + retVal + ", expected " + keyVal.get(key));
		  }
	  }
	  return ret;
  }
  private static String randomString(int len, int flag){
	  StringBuilder sb = new StringBuilder(len);
	  if(flag == 0){
		  sb.append("key-");
	  }else if(flag == 1)
		  sb.append("val-");
	  for( int i = 0; i < len - 4; i++ ) 
		  sb.append(AB.charAt( rnd.nextInt(AB.length()) ) );
	  return sb.toString();
  }
  
  static class clientThread implements Runnable{
	KeyValueService.Client client;
	int clientNum;
	String host;
	Integer port;
	long putTime, getTime;
	public clientThread(int clientNum, String host, Integer port){
		//this.client = client;
		this.host = host;
		this.port = port;
		client = startClient(host, port);
		this.clientNum = clientNum;
	}
	
	long getPutTime(){return putTime;}
	long getGetTime(){return getTime;}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		List<String> putKey = new ArrayList<String>();
    	List<String> putVal = new ArrayList<String>();
    	
    	for(int i = 0; i < listLen; i++){
    		String key1 = randomString(keyLen, 0);
    		String val1 = randomString(valLen, 1);
    		putKey.add(key1);
    		putVal.add(val1);
    		keyVal.put(key1, val1);
    		
    		//System.out.println("Thread " + clientNum + " Put: " + key1 + ", " + val1);
    	}
    	int numOps = 1000;
    	for(int k = 0; k < numOps; k++){
    		List<String> getKey = new ArrayList<String>();
        	for(int i = 0; i < listLen / 2; i++){
        		String key2 = randomString(keyLen, 0);
        		//String val = randomString(valLen, 1);
        		getKey.add(key2);
        		getKey.add(putKey.get(i));
        		
        		//listVal.add(val);
        		//keyVal.put(key, val);
        	}
        	for(String s : getKey){
        		//System.out.println("Thread " + clientNum + " Get: " + s);
        	}
        	
        	List<String> listRet = new ArrayList<String>();
        	
        	try{
        		long startTime = System.nanoTime();
        		client.multiPut(putKey, putVal);
        		long singlePut = System.nanoTime() - startTime;
        		putTime += singlePut;
        		//System.out.println(clientNum + "Put: " + latencyPut + "ms");
        	}catch (Exception e){
        		log.error("Exception in thread " + clientNum + " multiPut");
        	}
        	
        	try{
        		long startTime = System.nanoTime();
        		listRet = client.multiGet(getKey);
        		long singleGet = System.nanoTime() - startTime;
        		getTime += singleGet;
        		//System.out.println(clientNum + "Get: " + latencyGet + "ms");
        	}catch(Exception e){
        		log.error("Exception in thread " + clientNum + " multiGet");
        	}
        	
        	try{
        		Thread.sleep(10);
        	}catch(InterruptedException ex) {
        	    Thread.currentThread().interrupt();
        	}
        	
    	}
    	
  		float putLatency = (float) putTime / numOps;
  		float getLatency = (float) getTime / numOps;
  		System.out.println(putLatency + ", " + getLatency);
  		/*
        for(String s : listRet){
        	  System.out.println(s + " ");
        }
        */
        //boolean flag = compare(keyVal, getKey, listRet);
        //System.out.println(flag);
	}
	  
  }
}

