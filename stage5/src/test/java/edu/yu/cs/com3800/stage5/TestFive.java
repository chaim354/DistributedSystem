package edu.yu.cs.com3800.stage5;
//collabed with nir solooki



import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Client.Response;
import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class TestFive {
private  final int NUM_NODES = 8;
private  final int[] ports = {8100,8010, 8020, 8030, 8040, 8050, 8060, 8070};
private  final int[] IDs = {-1, 0, 1, 2, 3, 4, 5,6};
private Logger logger;
private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
@Test
public void testCluster() throws Exception {
    FileHandler fh;  
    Logger logger = Logger.getLogger("test5");
    fh = new FileHandler("output"+".log");
    logger.addHandler(fh);
    SimpleFormatter formatter = new SimpleFormatter();  
    fh.setFormatter(formatter); 
    logger.info("logger works");
    // Create a cluster of 7 nodes and one gateway, starting each in their own JVM
        //create IDs and addresses
    List<Process> processes = new ArrayList<>();
    for (int i = 0; i < IDs.length; i++) {
        int id = IDs[i]; 
        logger.info("process  = " + id);
        ProcessBuilder pb = new ProcessBuilder("javac", "-cp", "src/main/java:src/test/java", "src/test/java/ServerRunner.java");
        pb = new ProcessBuilder("java", "-cp","target/classes:target/test-classes","ServerRunner",((Integer)id).toString());
        Process p = pb.inheritIO().start();
        if(!p.isAlive()){
        }
        processes.add(p);
        int finalI = i;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                processes.get(finalI).destroy();
            }));
    }
    Thread.sleep(1000);
    // Wait until the election has completed before sending any requests to the Gateway
    // In order to do this, you must add another http based service to the Gateway which can be called to ask if it has a leader or not
    String leaderUrl = "http://localhost:8098/hasleader";
    while (true) {
        HttpURLConnection connection = (HttpURLConnection) new URL(leaderUrl).openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        if (responseCode == 200) {
            // If the Gateway has a leader, it should respond with the full list of nodes and their roles (follower vs leader)
            // Script should print out the list of server IDs and their roles
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            logger.info(response.toString());
            break;
        }
        Thread.sleep(1000);
    }

    // Once the gateway has a leader, send 9 client requests. Script should print out both the request and the response from the cluster
    String requestUrl = "http://localhost:8098/compileandrun";
    for (int i = 0; i < 9; i++) {
        String code = this.validClass.replace("world!", "world! from code version " + i);
        HttpURLConnection connection = (HttpURLConnection) new URL(requestUrl).openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "text/x-java-source");
        connection.setDoOutput(true);
        OutputStream os = connection.getOutputStream();
        os.write((code).getBytes());
        os.flush();
        os.close();

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
    }

    // kill -9 a follower JVM, printing out which one you are killing
    int killedPort = ports[2]; // for example
    processes.get(2).destroyForcibly();
    logger.info("Killed node on port " + killedPort);

    // Wait heartbeat interval * 10 time, and then retrieve and display the list of nodes from the Gateway
    // The dead node should not be on the list
    Thread.sleep(50000); // wait heartbeat interval * 10 time
    HttpURLConnection connection = (HttpURLConnection) new URL(leaderUrl).openConnection();
    connection.setRequestMethod("GET");
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    String inputLine;
    StringBuilder response = new StringBuilder();
     while ((inputLine = in.readLine()) != null) {
         response.append(inputLine);
     }
     in.close();
    logger.info(response.toString());

    // kill -9 the leader JVM and then pause 1000 milliseconds
    int leaderPort = ports[NUM_NODES - 1]; // assuming the last node is the leader
   processes.get(NUM_NODES - 1).destroyForcibly();
    logger.info("Killed leader node on port " + leaderPort);
    Thread.sleep(1000);

    // Send/display 9 more client requests to the gateway, in the background
    ExecutorService executor = Executors.newFixedThreadPool(9);
    CountDownLatch latch = new CountDownLatch(9);
    for (int i = 9; i < 18; i++) {
        final int j = i;
        executor.submit(() -> {
            try {
        final  String code = this.validClass.replace("world!", "world! from code version " + j);
        HttpURLConnection connectionOne = (HttpURLConnection) new URL(requestUrl).openConnection();
        connectionOne.setRequestMethod("POST");
        connectionOne.setRequestProperty("Content-Type", "text/x-java-source");
        connectionOne.setDoOutput(true);
        OutputStream os = connectionOne.getOutputStream();
        os.write((code).getBytes());
        os.flush();
        os.close();

        BufferedReader ins = new BufferedReader(new InputStreamReader(connectionOne.getInputStream()));
        String inputLines;
        StringBuilder responses = new StringBuilder();
        while ((inputLines = ins.readLine()) != null) {
            responses.append(inputLines);
        }
        ins.close();
        logger.info("Request " + j + ": " + responses.toString());
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
            });
            }

// // Wait for the Gateway to have a new leader, and then print out the node ID of the leader
// // Print out the responses the client receives from the Gateway for the 9 requests sent in step 6. Do not proceed to step 8 until all 9 requests have received responses.
Thread.sleep(50000);
while (true) {
     logger.info("into heres");
    connection = (HttpURLConnection) new URL(leaderUrl).openConnection();
     connection.setRequestMethod("GET");
     int responseCode = connection.getResponseCode();
     logger.info(responseCode + "");
     if (responseCode == 200) {
         in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
         inputLine = null;
         response = new StringBuilder();
         while ((inputLine = in.readLine()) != null) {
             response.append(inputLine);
        }
         in.close();
         logger.info(response.toString());
         latch.await();
         break;
     }
     Thread.sleep(1000);
 }

// Send/display 1 more client request (in the foreground), print the response
String code = this.validClass.replace("world!", "world! from code version " + 18);
connection = (HttpURLConnection) new URL(requestUrl).openConnection();
connection.setRequestMethod("POST");
connection.setRequestProperty("Content-Type", "text/x-java-source");
connection.setDoOutput(true);
OutputStream os = connection.getOutputStream();
os.write((code).getBytes());
os.flush();
os.close();

 in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
response = new StringBuilder();
while ((inputLine = in.readLine()) != null) {
    response.append(inputLine);
}
in.close();
logger.info("Request " + 18 + ": " + response.toString());

// List the paths to files containing the Gossip messages received by each node
//  String gossipUrl = "http://localhost:" + GATEWAY_PORT + "/getSummary";
//  for (int i = 0; i < NUM_NODES; i++) {
//      int port = NODE_PORTS[i]-4;
//      connection = (HttpURLConnection) new URL("http://localhost:" + port + "/getSummary").openConnection();
//      connection.setRequestMethod("GET");
//      in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
//      inputLine = null;
//      response = new StringBuilder();
//     while ((inputLine = in.readLine()) != null) {
//          response.append(inputLine);
//      }
//     in.close();
//      logger.info("Node on port " + port + ": " + response.toString());
// }

// Shut down all the nodes
for (Process process : processes) {
    process.destroyForcibly();
}
}

}

