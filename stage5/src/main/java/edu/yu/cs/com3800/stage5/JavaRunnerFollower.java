package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.TCPServer.TcpMessage;;

public class JavaRunnerFollower extends Thread implements LoggingServer{
private int port;
private String host;
private Logger logger;
private ZooKeeperPeerServer zserver;
private HashMap<Long,Message> compWork;
private TCPServer server;
    public JavaRunnerFollower(int port,String host, ZooKeeperPeerServer server, TCPServer tcpServer) {
        this.port = port+2;
        this.host = host;
        this.zserver=server;
        this.compWork = new HashMap<>();
        this.setDaemon(true);
        this.server = tcpServer;
    }

    @Override
    public void run() {
        ConcurrentLinkedQueue<TcpMessage> queue = server.getQueue();
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName()+zserver.getUdpPort());
                }
            } catch (IOException e) {
                this.logger.log(Level.WARNING,"failed to send packet", e);
            }
            TcpMessage tcpMessage = null;
                while (tcpMessage == null) {
                    try {
                        tcpMessage = queue.poll();
                    } catch (Exception e) {
                    }
                }
            if(zserver.isPeerDead(((InetSocketAddress)tcpMessage.s.getLocalSocketAddress()))){
                continue;
            }
            if(tcpMessage.m.getMessageType().equals(MessageType.NEW_LEADER_GETTING_LAST_WORK)){
                for(Map.Entry<Long,Message> i : compWork.entrySet()){

                Message out = new Message(Message.MessageType.COMPLETED_WORK, i.getValue().getMessageContents(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(), tcpMessage.s.getInetAddress().getHostName(), tcpMessage.s.getPort(), tcpMessage.m.getRequestID());
                try {
                    tcpMessage.s.getOutputStream().write(out.getNetworkPayload());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                compWork.remove(i.getKey());
            }
                continue;
            }
            ByteArrayInputStream inStream = new ByteArrayInputStream(tcpMessage.m.getMessageContents());
            JavaRunner jr = null;
            try {
                jr = new JavaRunner();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            String output = "n";
            boolean failed = false;
            try {
                output = jr.compileAndRun(inStream);
            } catch (Exception e) {
                failed=true;
                e.printStackTrace();
                output = e.getMessage();
            }
            Message out = new Message(Message.MessageType.COMPLETED_WORK, output.getBytes(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(), tcpMessage.s.getInetAddress().getHostName(), tcpMessage.s.getPort(), tcpMessage.m.getRequestID(),failed);
            if(zserver.isPeerDead(((InetSocketAddress)tcpMessage.s.getLocalSocketAddress()))){
                compWork.put(((Long)tcpMessage.m.getRequestID()),out);
                continue;
            }
            try {
                tcpMessage.s.getOutputStream().write(out.getNetworkPayload());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        interrupt();
    }
    
}
