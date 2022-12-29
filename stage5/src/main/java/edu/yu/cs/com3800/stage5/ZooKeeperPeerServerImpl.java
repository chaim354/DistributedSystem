package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage5.TCPServer.TcpMessage;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.io.IOException;;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    public Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private Logger logger;
    private HashSet<Long> deadSet = new HashSet<>();
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private JavaRunnerFollower jFollower;
    private RoundRobinLeader robinLeader;
    private Long gatewayID;
    private int numberOfObservers;
    private Heartbeat heart;
    private TCPServer tcpServer;
    public ZooKeeperPeerServerImpl(int udpPort, long peerEpoch, Long serverID, ConcurrentMap<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        this.numberOfObservers =numberOfObservers; 
        this.myPort = udpPort;
        this.peerEpoch = peerEpoch;
        this.gatewayID = gatewayID;
        this.id = serverID;
        this.peerIDtoAddress =  (ConcurrentHashMap<Long, InetSocketAddress>) peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", this.myPort);
        if (peerIDtoAddress.get(this.id) == null) {
            this.peerIDtoAddress.put(this.id, this.myAddress);
        }
        this.state = ServerState.LOOKING;
        this.currentLeader = null;
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();

        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        this.robinLeader = null;
        this.jFollower = null;
        this.heart = new Heartbeat(incomingMessages, outgoingMessages, this.peerIDtoAddress, this);
        this.tcpServer = new TCPServer(this.myPort+2, new ConcurrentLinkedQueue<TcpMessage>());
        try {
            this.logger = initializeLogging(
                    ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-port-" + this.myPort);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        logger.info("server starting");
        // step 1: create and run thread that sends broadcast messages
        this.senderWorker.start();
        // step 2: create and run thread that listens for messages sent to this server
        this.receiverWorker.start();
        // step 3: main server loop
        try {
            while (!this.shutdown) {
                switch (getPeerState()) {
                    case LOOKING:
                        // start leader election, set leader to the election winner
                       // Thread.sleep(5000);
                        logger.info(this.id + "in election");
                        if(currentLeader==null){
                            setCurrentLeader(startLeaderElection());
                            if(peerEpoch==0){
                                heart.start();
                            }
                        }
                        // TODO start gossiper need to change for second election, how do you pause election?
                        break;
                    case LEADING:
                        if(this.robinLeader == null) {
                            if(this.jFollower!=null){
                                this.jFollower.shutdown();
                            } else {
                                tcpServer.start();
                            }
                        this.robinLeader = new RoundRobinLeader(peerIDtoAddress,myAddress,gatewayID,this,tcpServer);
                        robinLeader.start();
                        }
                        break;
                    case FOLLOWING:
                    if(this.jFollower==null){
                        tcpServer.start();
                        this.jFollower = new JavaRunnerFollower(myPort,myAddress.getHostName(),this,tcpServer);
                        jFollower.start();
                        }
                        break;
                    case OBSERVER:
                    if(currentLeader==null){

                        setCurrentLeader(startLeaderElection());
                        if(peerEpoch==0){
                            heart.start();
                        }
                    }
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            // code...
        }
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if(jFollower!=null){
        this.jFollower.shutdown();}
        if(this.robinLeader!=null){
        this.robinLeader.shutdown();}
        this.heart.interrupt();
        logger.info("server shutting down " + myAddress.getPort());

    }

    private Vote startLeaderElection() {
        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, this.incomingMessages);
        return election.lookForLeader();
    }

    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(),
                target.getPort()));
    }

    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for (InetSocketAddress target : peerIDtoAddress.values()) {
            if (!target.equals(this.myAddress)) {
                sendMessage(type, messageContents, target);
            }
        }
    }

    public ServerState getPeerState() {
        return this.state;
    }

    public void setPeerState(ServerState newState) {
        this.state = newState;
    }

    public Long getServerId() {
        return this.id;
    }

    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    public int getUdpPort() {
        return this.myPort;
    }

    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    public int getQuorumSize() {
        return ((peerIDtoAddress.keySet().size()-numberOfObservers)/2);
    }
    @Override
    public void reportFailedPeer(long peerID){
        deadSet.add(peerID);
        if(robinLeader!=null){
            robinLeader.redistributeWork(peerID);
            }
        peerIDtoAddress.remove(peerID);
            logger.info(peerID + " was marked as dead");
        if(currentLeader.getProposedLeaderID()==peerID){
            peerEpoch++;
            this.currentLeader=null;
            if(!this.state.equals(ServerState.OBSERVER)){
            setPeerState(ServerState.LOOKING);
            }

        }
    }
    @Override
    public  boolean isPeerDead(long peerID){
        return deadSet.contains(peerID);
    }
    @Override
    public  boolean isPeerDead(InetSocketAddress address){
        long peerID = 0L;
        for(Map.Entry<Long,InetSocketAddress> i: peerIDtoAddress.entrySet()){
            
            if(i.getValue().equals(address)){
                peerID = i.getKey();
                break;
            }
        }

        return deadSet.contains(peerID);
    }

    public Long idFromPort(int port){
        for(Map.Entry<Long,InetSocketAddress> i: peerIDtoAddress.entrySet()){
            
            if(i.getValue().getPort()==port){
                return i.getKey();
            }
        }
        return null;
    }

    public void reportFailedPeer(long peerID, Logger logger2) {
        deadSet.add(peerID);
        if(robinLeader!=null){
            robinLeader.redistributeWork(peerID);
            }
        peerIDtoAddress.remove(peerID);

        if(currentLeader.getProposedLeaderID()==peerID){
            peerEpoch++;
            this.currentLeader=null;
            if(!this.state.equals(ServerState.OBSERVER)){
            logger2.info("["+this.id+"]: switching from ["+this.state+"] to ["+ServerState.LOOKING+"]");
            setPeerState(ServerState.LOOKING);
            }

        }
    }

}
