package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import java.util.logging.Logger;
import java.util.concurrent.*;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.stage5.TCPServer.TcpMessage;;

public class RoundRobinLeader extends Thread implements LoggingServer{
    private final InetSocketAddress myAddress;
    private Logger logger;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private ConcurrentHashMap<Integer,List<TcpMessage>> requestMap;
    private ConcurrentLinkedQueue<TcpMessage> queue;
    private ConcurrentHashMap<Long,Message> oldWork;
    private TCPServer tcpServer;
    private Long gatewayId;
    private ZooKeeperPeerServerImpl zserver;
    private  int it = 0;
    private  ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*3); 
    public RoundRobinLeader( Map<Long, InetSocketAddress> peerIDtoAddress,InetSocketAddress myAddress, Long gatewayId, ZooKeeperPeerServerImpl zserver, TCPServer tcpServer) {
        this.oldWork = new ConcurrentHashMap<>();
        this.zserver = zserver;
        this.peerIDtoAddress = peerIDtoAddress;
        this.requestMap = new ConcurrentHashMap<>();
        this.myAddress = myAddress;
        this.gatewayId = gatewayId;
        this.setDaemon(true);
        this.tcpServer = tcpServer;
        this.queue = tcpServer.getQueue();
        try {
            if(this.logger == null){
                this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + zserver.getUdpPort());
            }
        } catch (IOException e) {
            this.logger.log(Level.WARNING,"failed to send packet", e);
        }
    }
    @Override
    public void run() {
        if(zserver.getPeerEpoch()>0){
            for(Entry<Long,InetSocketAddress> i: peerIDtoAddress.entrySet()){
                if(i.getValue().equals(myAddress)||i.getKey().equals(gatewayId)){
                    continue;
                }
                Thread t = new TCPClient(i.getValue().getPort()+2,  i.getValue().getHostName(), tcpServer.new TcpMessage(null, new Message(null, new byte[0], null, 0, myAddress.getHostString(), myAddress.getPort())),requestMap,MessageType.NEW_LEADER_GETTING_LAST_WORK,oldWork,logger);
                    pool.execute(t);
            }
        }
        while (!this.isInterrupted()) {

            ArrayList<Map.Entry<Long, InetSocketAddress>> arr = new ArrayList<>(peerIDtoAddress.entrySet());
                if(arr.get(it).getValue().equals(myAddress)||arr.get(it).getKey().equals(gatewayId)){
                    it = (it+1)%arr.size();
                    continue;
                }
                if(zserver.isPeerDead(arr.get(it).getKey())){
                    arr.remove(it);
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
                    Thread t = new TCPClient(arr.get(it).getValue().getPort()+2,  arr.get(it).getValue().getHostName(), tcpMessage,requestMap,MessageType.WORK,new ConcurrentHashMap<Long,Message>(),logger);
                    pool.execute(t);
                    it = (it+1)%arr.size();
                    
                if(this.isInterrupted()){
                    break;
                }
        }
    }

    public void redistributeWork(Long peerID){
        int port = peerIDtoAddress.get(peerID).getPort();
        peerIDtoAddress.remove(peerID);
        List<TcpMessage> list = requestMap.get(port);
        if(list==null||list.isEmpty()){
            return;
        }
        for(TcpMessage i: list){
            ArrayList<Map.Entry<Long, InetSocketAddress>> arr = new ArrayList<>(peerIDtoAddress.entrySet());
            while(true) {
                if(!arr.get(it).getValue().equals(myAddress)&&!arr.get(it).getKey().equals(gatewayId)&&!zserver.isPeerDead(arr.get(it).getKey())){
                    break;
                }else{
                    it = (it+1)%arr.size();
                }
            }
                    Thread t = new TCPClient(arr.get(it).getValue().getPort()+2,  arr.get(it).getValue().getHostName(), i,requestMap,MessageType.WORK,new ConcurrentHashMap<Long,Message>(),logger);
                    pool.execute(t);
                    it = (it+1)%arr.size();

        }
    }
    
    public void shutdown() {
        interrupt();
    }
}
