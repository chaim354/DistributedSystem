package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.TCPServer.TcpMessage;


public class TCPClient extends Thread{
    private TcpMessage tcpMessage;
    private Socket socket;
    private int port;
    private String host;
    private ConcurrentHashMap<Integer,List<TcpMessage>> requestMap;
    private ConcurrentHashMap<Long,Message> oldWork;
    private Message.MessageType mType;
    private Logger logger;
    public TCPClient(int port,String host, TcpMessage tcpMessage, ConcurrentHashMap<Integer,List<TcpMessage>> requestMap, Message.MessageType mType,ConcurrentHashMap<Long,Message> oldWork, Logger logger) {
        this.requestMap = requestMap;
        this.tcpMessage = tcpMessage;
        this.port = port;
        this.host = host;
        this.mType = mType;
        this.oldWork = oldWork;
        this.logger = logger;
        setDaemon(true);
    }

    @Override
    public void run(){
        try {
            socket =  new  Socket(host, port);
        } catch (IOException e) {
            // TODO Auto-generated catch block
        }
        Message m = new Message(mType, tcpMessage.m.getMessageContents(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(),host, port, tcpMessage.m.getRequestID());
       logger.info("sending work to follower");
        try {
            socket.getOutputStream().write(m.getNetworkPayload());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if(mType.equals(MessageType.NEW_LEADER_GETTING_LAST_WORK)){
            byte[] o = null;
            try {
                o = Util.readAllBytesFromNetwork(socket.getInputStream());
                socket.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            
            Message n = new Message(o);
            oldWork.put(n.getRequestID(), n);
            // wait for response
            // add to hashmap
        } else {
        Message old = oldWork.get(tcpMessage.m.getRequestID());
        if(old!=null){
            oldWork.remove(tcpMessage.m.getRequestID());
            Message out = new Message(Message.MessageType.COMPLETED_WORK, old.getMessageContents(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(), tcpMessage.s.getInetAddress().getHostName(), tcpMessage.s.getPort(), tcpMessage.m.getRequestID());
        try {
            tcpMessage.s.getOutputStream().write(out.getNetworkPayload());
            tcpMessage.s.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        }
        ArrayList<TcpMessage> list = new ArrayList<>();
        list.add(tcpMessage);
        if(requestMap.putIfAbsent(tcpMessage.m.getReceiverPort(), list)!=null){
            list = (ArrayList)requestMap.get(tcpMessage.m.getReceiverPort());
            list.add(tcpMessage);
            requestMap.put(tcpMessage.m.getReceiverPort(), list);
        }
        byte[] o = null;
        try {
            o = Util.readAllBytesFromNetwork(socket.getInputStream());
            socket.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        Message n = new Message(o);
        Message out = new Message(Message.MessageType.COMPLETED_WORK, n.getMessageContents(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(), tcpMessage.s.getInetAddress().getHostName(), tcpMessage.s.getPort(), tcpMessage.m.getRequestID());
        try {
            logger.info("response recieved from worker" + n.getMessageContents().toString());
            tcpMessage.s.getOutputStream().write(out.getNetworkPayload());
            tcpMessage.s.close();
            list = (ArrayList)requestMap.get(tcpMessage.m.getReceiverPort());
            list.remove(tcpMessage);
            requestMap.put(tcpMessage.m.getReceiverPort(), list);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    }
    
}
