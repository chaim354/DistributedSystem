package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.*;
import java.util.Map.Entry;
import java.io.Serializable;

import com.google.inject.OutOfScopeException;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

public class Heartbeat extends Thread implements LoggingServer{
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private Map<Long,Beat> heartMap = new HashMap<>();
    private ZooKeeperPeerServerImpl server;
    static final int GOSSIP = 3000; 
    static final int FAIL = GOSSIP * 10; 
    static final int CLEANUP = FAIL * 2;
    private boolean justPaused;
    private Long count = 0L;
    private Logger logger;
    private HttpServer httpServer;
    private ByteArrayOutputStream out;
    private List<Entry<Long, InetSocketAddress>> queue = new LinkedList<>();
    public Heartbeat(LinkedBlockingQueue<Message> incomingMessages,LinkedBlockingQueue<Message> outgoingMessages,ConcurrentHashMap<Long,InetSocketAddress> peerIDtoAddress, ZooKeeperPeerServerImpl server) {
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.server = server;
        try {
            this.logger = initializeLogging("gossiperNode:" + this.server.getServerId());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.out = new ByteArrayOutputStream();
        StreamHandler sh = new StreamHandler(out, new SimpleFormatter());
        this.logger.addHandler(sh);
    }

    @Override
    public void run(){
        try {
            this.httpServer = httpServer.create(new InetSocketAddress(server.getUdpPort()-4), 0);
        } catch (IOException e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }
        try {
            HttpContext cOne =  httpServer.createContext("/getSummary", new MyHandler());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
        //loop though senbd message wait for response if no response dead do appropriate things 
        Long t = System.currentTimeMillis();
        for (Entry<Long, InetSocketAddress> i:peerIDtoAddress.entrySet()){
            heartMap.put(i.getKey(), new Beat(t, 0L, i.getValue()));
            queue.add(i);
        }
        Collections.shuffle(queue);
        while(!this.interrupted()){
            if(server.getCurrentLeader()==null){
                justPaused=true;
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                continue;
            }
            if(justPaused){
                justPaused=false;
                fixMap();
            }
            long time = System.currentTimeMillis();
            mergeData(time);
            checkFailure(time);
            checkCleanup(time);
            count++;
            Entry<Long, InetSocketAddress> i = queue.remove(0);
            queue.add(i);
            heartMap.put(server.id, new Beat(time, count, server.getAddress()));
            server.sendMessage(Message.MessageType.GOSSIP, buildMsgContent(), i.getValue());
            //filter
            try {
                Thread.sleep(GOSSIP);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }



        }

    }

    private void mergeData(long time){
        for(Message m: incomingMessages){
            if(m.getMessageType().equals(MessageType.ELECTION)){
                continue;
            }
            Map<Long,Beat> recievedMap = fromMessage(m);
            for(Map.Entry<Long,Beat> b:recievedMap.entrySet()){
                Beat beat = heartMap.get(b.getKey());
                if(beat == null){
                }
                if(b.getValue().Count>heartMap.get(b.getKey()).Count){
                    heartMap.put(b.getKey(), new Beat(time, b.getValue().Count, b.getValue().addr()));
                    logger.info( "[" + server.id + "]: updated ["+b.getKey()+"]’s heartbeat sequence to ["+b.getValue().Count+"] based on message from ["+server.idFromPort(m.getSenderPort())+"] at node time ["+time+"]");
                }
            }
            this.incomingMessages.remove(m);
        }
    }

    private void checkFailure(long time){
        for(Map.Entry<Long,Beat> b:heartMap.entrySet()){
            if(time-b.getValue().time>=FAIL && !server.isPeerDead(b.getKey())){
                logger.info("[" + server.id + "]: no heartbeat from server ["+b.getKey()+"] - SERVER FAILED");
                server.reportFailedPeer(b.getKey(),logger);
            }
    }
    }

    private void checkCleanup(long time){
        for(Map.Entry<Long,Beat> b:heartMap.entrySet()){
            if(time-b.getValue().time>=CLEANUP && !server.isPeerDead(b.getKey())){
                heartMap.remove(b.getKey());
                queue.remove(b);
            }
        }
    }
    private void fixMap(){
        long time = System.currentTimeMillis();
        for(Map.Entry<Long,Beat> i: heartMap.entrySet()){
            heartMap.put(i.getKey(), new Beat(time, i.getValue().Count, i.getValue().addr));
        }
    }

     byte[] buildMsgContent() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(baos);
            out.writeObject(heartMap);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return baos.toByteArray();
    }

    private Map<Long,Beat> fromMessage(Message m) {
        ByteArrayInputStream bais = new ByteArrayInputStream(m.getMessageContents());

      // create an object input stream to deserialize the map
      ObjectInputStream ois = null;
    try {
        ois = new ObjectInputStream(bais);
    } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
    }

      // read and return the map
      try {
        return (Map<Long, Heartbeat.Beat>) ois.readObject();
    } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    return null;
    }
    
    class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String response = out.toByteArray().toString(); 
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }



    public record Beat(Long time, Long Count, InetSocketAddress addr) implements Serializable{}
    
}
