package edu.yu.cs.com3800.stage4;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Client.Response;
import edu.yu.cs.com3800.stage4.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage4.GatewayServer;
import edu.yu.cs.com3800.stage4.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
public class Stage4Test {
    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    //private int[] ports = {8010, 8020};
    private int leaderPort = this.ports[this.ports.length - 1];
    private int myPort = 9999;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ArrayList<ZooKeeperPeerServer> servers;
    private GatewayServer gateway;

    @Test
    public void DemoTest() throws Exception {
        System.out.println("test starting");
        //step 1: create sender & sending queue
        this.outgoingMessages = new LinkedBlockingQueue<>();
        UDPMessageSender sender = new UDPMessageSender(this.outgoingMessages,this.myPort);
        //step 2: create servers
        createServers();
        System.out.println("Servers created");
        //step2.1: wait for servers to get started

        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
        }
       printLeaders();
        //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
        for (int i = 0; i < this.ports.length; i++) {
            String code = this.validClass.replace("world!", "world! from code version " + i);
            System.out.println(i+ "testrun");
            sendMessage(code);
        }
        
    }

    private void printLeaders() {
        for (ZooKeeperPeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }
    }

    private void stopServers() {
        for (ZooKeeperPeerServer server : this.servers) {
            server.shutdown();
        }
    }

    private void printResponses() throws Exception {
        String completeResponse = "";
        for (int i = 0; i < this.ports.length; i++) {
            Message msg = this.incomingMessages.take();
            String response = new String(msg.getMessageContents());
            completeResponse += "Response to request " + msg.getRequestID() + ":\n" + response + "\n\n";
        }
        System.out.println(completeResponse);
    }

    private void sendMessage(String code) throws InterruptedException {
        Client client = null;
        Response r = null;
        try {
            client = new ClientImpl("localhost", 8098);
            
            client.sendCompileAndRunRequest(code);
            
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            r = client.getResponse();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(r.getBody());
        //Thread.sleep(5000);
    }

    private void createServers() throws IOException {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }
        
        //create servers
        this.servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            map.put(-1L, new InetSocketAddress("localhost", 8100));
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map,-1L,1);
            this.servers.add(server);
            
            server.start();
        }
        System.out.println("before gatway");
        gateway = new GatewayServer(8100, 0, -1L, peerIDtoAddress, 1);
    }
}
