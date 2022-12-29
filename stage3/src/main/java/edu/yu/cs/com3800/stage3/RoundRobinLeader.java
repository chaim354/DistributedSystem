package edu.yu.cs.com3800.stage3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;;

public class RoundRobinLeader extends Thread implements LoggingServer{
    private final InetSocketAddress myAddress;
    private Long requestCount = 0L;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    private Logger logger;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private Map<Long,InetSocketAddress> requestMap;
    public RoundRobinLeader(LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages, Map<Long, InetSocketAddress> peerIDtoAddress,InetSocketAddress myAddress) {
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.peerIDtoAddress = peerIDtoAddress;
        this.requestMap = new HashMap<>();
        this.myAddress = myAddress;
        this.setDaemon(true);
    }
    @Override
    public void run() {
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
                if(arr.get(it).getValue().equals(myAddress)){
                    it = (it+1)%arr.size();
                    continue;
                }
                Message message = null;
                while (message == null) {
                    try {
                        message = incomingMessages.take();
                    } catch (Exception e) {
                    }
                }
                if(message.getMessageType()==Message.MessageType.WORK){
                    Long requestID = requestCount;
                    requestCount++;
                    requestMap.put(requestID, new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
                    outgoingMessages.offer(new Message(Message.MessageType.WORK, message.getMessageContents(), message.getReceiverHost(), message.getReceiverPort(), arr.get(it).getValue().getHostName(), arr.get(it).getValue().getPort(), requestID));
                    it = (it+1)%arr.size();
                    

                } else{
                    outgoingMessages.offer(new Message(Message.MessageType.COMPLETED_WORK, message.getMessageContents(), message.getReceiverHost(), message.getReceiverPort(), requestMap.get(message.getRequestID()).getHostName(), requestMap.get(message.getRequestID()).getPort(),message.getRequestID()));

                }
                if(this.isInterrupted()){
                    break;
                }
        }
    }
    
    public void shutdown() {
        interrupt();
    }
}
