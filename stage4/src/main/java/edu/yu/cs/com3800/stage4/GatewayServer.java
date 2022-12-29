package edu.yu.cs.com3800.stage4;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.LoggingServer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.util.*;
import java.util.logging.*;
import edu.yu.cs.com3800.*;
public class GatewayServer implements LoggingServer{
    private int port;
    private HttpServer server;
    private  Logger logger;
    GatewayPeerServerImpl peerServer;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private int requestCount;

    public GatewayServer(int port,  long peerEpoch, Long id,  Map<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers){
        this.peerIDtoAddress = peerIDtoAddress;
        try {
                this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName());
        } catch (IOException e) {
        }
        this.port = port;
        try {
            this.server = server.create(new InetSocketAddress(port-2), 0);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        server.createContext("/compileandrun", new MyHandler());
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*3));
        start();
        this.peerServer = new GatewayPeerServerImpl(port, peerEpoch, id, peerIDtoAddress, id, numberOfObservers);
        peerServer.start();
    }

     /**
     * start the server
     */
    public void start(){
        logger.info("server starting");
        server.start();
    }

    /**
     * stop the server
     */
    public void stop(){
        logger.info("server stopping");
        server.stop(0);
    }
    


    class MyHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            requestCount++;
            logger.info(String.valueOf("request count: " + requestCount));
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
            List<Byte> output = new ArrayList<>();

            while(peerServer.getCurrentLeader()== null){
            }
            try (Socket socket = new Socket(peerServer.getPeerByID(peerServer.getCurrentLeader().getProposedLeaderID()).getAddress(),peerServer.getPeerByID(peerServer.getCurrentLeader().getProposedLeaderID()).getPort()+2)) {

                Message m = new Message(Message.MessageType.WORK, Util.readAllBytesFromNetwork(is), socket.getLocalAddress().getHostName(), socket.getLocalPort(), socket.getInetAddress().getHostName(), socket.getPort(), requestCount);
                socket.getOutputStream().write(m.getNetworkPayload());

    

                byte[] temp = Util.readAllBytesFromNetwork(socket.getInputStream());

                socket.close();
                Message out = new Message(temp);
                String result = new String(out.getMessageContents());
                t.sendResponseHeaders(200, result.length());
                OutputStream os = t.getResponseBody();
                os.write(result.getBytes());
                os.close();
                logger.info("code response: " + result);
            
            } catch (IOException ex) {
            } catch (NoSuchElementException e)
            {
                
            }


        }
    }
}
