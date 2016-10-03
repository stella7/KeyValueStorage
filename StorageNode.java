import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.*;
import org.apache.log4j.*;

public class StorageNode {
  static Logger log;

  public static void main(String [] args) throws Exception {
      if (args.length != 2) {
	  System.err.println("Usage: java StorageNode config_file node_num");
	  System.exit(-1);
      }

      // initialize log4j
      BasicConfigurator.configure();
      log = Logger.getLogger(StorageNode.class.getName());

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
      int myNum = Integer.parseInt(args[1]);
      log.info("Launching storage node " + myNum + ", " + hosts.get(myNum) + ":" + ports.get(myNum));
      
      KeyValueHandler kvHandler = new KeyValueHandler(myNum, hosts, ports);
      Thread thread = new Thread(new createConnection(myNum, kvHandler, hosts, ports));
      thread.start();
      
      KeyValueService.Processor<KeyValueService.Iface> processor =new KeyValueService.Processor<>(kvHandler);
      // Launch a Thrift server here
      /*
      TNonblockingServerSocket socket = new TNonblockingServerSocket(ports.get(myNum));
      THsHaServer.Args sargs = new THsHaServer.Args(socket);
      sargs.protocolFactory(new TBinaryProtocol.Factory());
      sargs.transportFactory(new TFramedTransport.Factory());
      sargs.processorFactory(new TProcessorFactory(processor));
      sargs.maxWorkerThreads(hosts.size() * 16);
      TServer server = new THsHaServer(sargs);
      */
      TNonblockingServerTransport socket = new TNonblockingServerSocket(ports.get(myNum));
      TThreadedSelectorServer.Args sargs = new TThreadedSelectorServer.Args(socket);
      sargs.transportFactory(new TFramedTransport.Factory());
      sargs.protocolFactory(new TBinaryProtocol.Factory());
      sargs.processorFactory(new TProcessorFactory(processor));
      sargs.selectorThreads(4);
      sargs.workerThreads(32);
      TServer server = new TThreadedSelectorServer(sargs);
      
      server.serve();
      thread.join();
      //throw new Error("This code needs more work!");
  }
  
  private static class createConnection implements Runnable {
      private KeyValueHandler kvHandler;
      private HashMap<Integer, String> hosts;
      private HashMap<Integer, Integer> ports;
      private int myNum;

      createConnection(int myNum, KeyValueHandler kvHandler, HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports) {
            this.kvHandler = kvHandler;
            this.hosts = hosts;
            this.ports = ports;
            this.myNum = myNum;
      }

      public void run() {
            for (Integer sn=0; sn < hosts.size(); sn++){
                  if (sn != myNum){
                        LinkedList<KeyValueService.Client> clients = new LinkedList<KeyValueService.Client>();
                        for(int i =0; i < 64; i++){
                              TSocket sock = new TSocket(hosts.get(sn), ports.get(sn));
                              TTransport transport = new TFramedTransport(sock);
                              TProtocol protocol = new TBinaryProtocol(transport);
                        
                              KeyValueService.Client client = new KeyValueService.Client(protocol);
                              clients.add(client);
                              boolean flag = false;
                              while(flag == false){
                                    try{
                                          flag = true;
                                          transport.open();
                                    }
                                    catch(Exception x){
                                          flag = false;
                                          continue;
                                    }
                              }
                        }     
                        kvHandler.addConnection(sn, clients);                        
                  }
            }
      }
  }
}
