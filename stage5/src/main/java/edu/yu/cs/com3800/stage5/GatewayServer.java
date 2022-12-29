package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.LoggingServer;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.*;
import edu.yu.cs.com3800.*;
public class GatewayServer implements LoggingServer{
    private int port;
    private HttpServer server;
    private  Logger logger;
    GatewayPeerServerImpl peerServer;
    private ConcurrentMap<Long, InetSocketAddress> peerIDtoAddress;
    private AtomicLong requestIDs;

    public GatewayServer(int port,  long peerEpoch, Long id,  ConcurrentMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers){
        this.peerIDtoAddress = peerIDtoAddress;
        try {
                this.logger = initializeLogging(GatewayServer.class.getCanonicalName());
        } catch (IOException e) {
        }
        logger.info("logger creation worked");
        this.port = port;
        try {
            this.server = server.create(new InetSocketAddress(port-2), 0);
        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }
        try {
            HttpContext cTwo = server.createContext("/hasleader", new HasLeaderHandler());
            HttpContext cOne =  server.createContext("/compileandrun", new MyHandler());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }

        
        logger.info("htttp server port: " + server.getAddress().getPort() + " host ; " + server.getAddress().getHostName());
        //logger.info("c and r" + cOne.getPath());
        //logger.info("h and l" + cTwo.getPath());

        requestIDs = new AtomicLong();
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*3));
        server.start();
        this.peerServer = new GatewayPeerServerImpl(port, peerEpoch, id, peerIDtoAddress, id, numberOfObservers);
        peerServer.start();
    }

     /**
     * start the server
     */
    public void start(){
        logger.info("server starting");
    }

    /**
     * stop the server
     */
    public void stop(){
        logger.info("server stopping");
        server.stop(0);
    }
    


    class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            logger.info(String.valueOf("request count: " + requestIDs));
            Map headers = t.getRequestHeaders();
            if(!(headers.get("Content-Type").toString().equals("[text/x-java-source]"))){
                String response = "incorrect content type";
                t.sendResponseHeaders(400, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
                logger.warning("request had wrong content type");
                return;
            }
            InputStream is = t.getRequestBody();
            byte[] work =  Util.readAllBytesFromNetwork(is);
            byte[] temp = toLeader(work, requestIDs.getAndIncrement());


    

                Message out = new Message(temp);
                String result = new String(out.getMessageContents());
                t.sendResponseHeaders(200, result.length());
                OutputStream os = t.getResponseBody();
                os.write(result.getBytes());
                os.close();
                logger.info("code response: " + result);
        }

        private byte[] toLeader(byte[] work,long id){
           // System.out.println(peerServer.getCurrentLeader().getProposedLeaderID()+ " start of to leader");
            while(peerServer.getCurrentLeader()== null){
            }
            try {
                Socket socket = new Socket(peerServer.getPeerByID(peerServer.getCurrentLeader().getProposedLeaderID()).getAddress(),peerServer.getPeerByID(peerServer.getCurrentLeader().getProposedLeaderID()).getPort()+2);
                Vote v = peerServer.getCurrentLeader();
                Message m = new Message(Message.MessageType.WORK, work, socket.getLocalAddress().getHostName(), socket.getLocalPort(), socket.getInetAddress().getHostName(), socket.getPort(), id);
                socket.getOutputStream().write(m.getNetworkPayload());
                byte[] temp = Util.readAllBytesFromNetwork(socket.getInputStream());
                if(peerServer.getCurrentLeader()== null || peerServer.isPeerDead(v.getProposedLeaderID())){
                    return toLeader(work, id);
                }
                socket.close();

                return temp;
            } catch (Exception e) {
                try {
                    Thread.sleep(35000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                while(peerServer.getCurrentLeader()== null){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                }
                return toLeader(work, id);
            }
        }
    }

    class HasLeaderHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            logger.info("making to hasleader");
            // Check if the peerServer has a leader
            if (peerServer.getCurrentLeader() != null) {
                // If there is a leader, return a 200 OK response with a list of nodes and their roles
                String response = "The gateway has a leader!\n" +
                    "Node IDs and roles:\n";
                for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
                    long id = entry.getKey();
                    String role = (id == peerServer.getCurrentLeader().getProposedLeaderID()) ? "leader" : "follower";
                    response += id + ": " + role + "\n";
                }
                t.sendResponseHeaders(200, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                // If there is no leader, return a 404 Not Found response
                String response = "The gateway does not have a leader yet.";
                t.sendResponseHeaders(404, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        }
    }

}
