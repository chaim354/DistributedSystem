package edu.yu.cs.com3800.stage4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import java.util.logging.Logger;
import edu.yu.cs.com3800.stage4.TCPServer.TcpMessage;
import java.util.concurrent.*;

import edu.yu.cs.com3800.*;;

public class RoundRobinLeader extends Thread implements LoggingServer{
    private final InetSocketAddress myAddress;
    private Long requestCount = 0L;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    private Logger logger;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private Map<Long,InetSocketAddress> requestMap;
    private ConcurrentLinkedQueue<TcpMessage> queue;
    private TCPServer tcpServer;
    private Long gatewayId;
    public RoundRobinLeader( Map<Long, InetSocketAddress> peerIDtoAddress,InetSocketAddress myAddress, Long gatewayId) {
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.requestMap = new HashMap<>();
        this.myAddress = myAddress;
        this.gatewayId = gatewayId;
        this.setDaemon(true);
        this.queue = new ConcurrentLinkedQueue<>();
        this.tcpServer = new TCPServer(myAddress.getPort()+2, queue);

    }
    @Override
    public void run() {
        tcpServer.start();
        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*3); 
        int it = 0;
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName());
                }
            } catch (IOException e) {
                this.logger.log(Level.WARNING,"failed to send packet", e);
            }
            ArrayList<Map.Entry<Long, InetSocketAddress>> arr = new ArrayList<>(peerIDtoAddress.entrySet());
                if(arr.get(it).getValue().equals(myAddress)||arr.get(it).getKey().equals(gatewayId)){
                    it = (it+1)%arr.size();
                    continue;
                }
                TcpMessage tcpMessage = null;
                while (tcpMessage == null) {
                    try {
                        tcpMessage = queue.poll();
                    } catch (Exception e) {
                    }
                }

                    Long requestID = requestCount;
                    requestCount++;
                    requestMap.put(requestID, new InetSocketAddress(tcpMessage.m.getSenderHost(), tcpMessage.m.getSenderPort()));
                    Thread t = new TCPClient(arr.get(it).getValue().getPort()+2,  arr.get(it).getValue().getHostName(), tcpMessage);
                    pool.execute(t);
                    it = (it+1)%arr.size();
                    
                if(this.isInterrupted()){
                    break;
                }
        }
    }
    
    public void shutdown() {
        interrupt();
    }
}
