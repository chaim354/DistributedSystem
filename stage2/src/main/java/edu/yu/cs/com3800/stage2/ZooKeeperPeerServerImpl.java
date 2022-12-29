package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.io.IOException;;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private Logger logger;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", this.myPort);
        if (peerIDtoAddress.get(this.id) == null) {
            this.peerIDtoAddress.put(this.id, this.myAddress);
        }
        this.state = ServerState.LOOKING;
        this.currentLeader = new Vote(this.id, this.peerEpoch);
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();
        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
        this.receiverWorker.setDaemon(true);
        this.senderWorker.setDaemon(true);
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
                        setCurrentLeader(startLeaderElection());
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
        logger.info("server shutting down");

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
        return peerIDtoAddress.keySet().size();
    }

}
