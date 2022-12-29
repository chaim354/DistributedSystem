package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;
    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServer myPeerServer;
    private long proLeader;
    private long proEpoch;
    private Map<Long, ElectionNotification> votes;
    /**
     * Upper bound on the amount of time between two consecutive notification
     * checks.
     * This impacts the amount of time to get the system up again after long
     * partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proLeader = server.getServerId();
        this.proEpoch = server.getPeerEpoch();
        this.votes = new HashMap<>();
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proLeader, this.proEpoch);
    }

    public synchronized Vote lookForLeader() {
        // send initial notifications to other peers to get things started
        sendNotifications();
        // Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING || this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.OBSERVER) {

            Message message = null;
            ElectionNotification en = null;
            int timeout = finalizeWait;

            while (message == null) {
                try {
                    message = incomingMessages.poll(timeout, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                }

                if (message == null) {

                    sendNotifications();

                    timeout = Math.min(timeout * 2, maxNotificationInterval);
                    continue;
                }
                en = getNotificationFromMessage(message);
            }
            switch (en.getState()) {
                case LOOKING: // if the sender is also looking

                    if (supersedesCurrentVote(en.getProposedLeaderID(), en.getPeerEpoch())) {
                        this.proEpoch = en.getPeerEpoch();
                        this.proLeader = en.getProposedLeaderID();
                        sendNotifications();
                    }

                    votes.put(en.getSenderID(), en);

                    if (haveEnoughVotes(votes, new Vote(this.proLeader, this.proEpoch))) {

                        if (!isHigherVote()) {
                            return acceptElectionWinner(en);
                        }

                    }
                    break;
                case FOLLOWING:
                case LEADING: // if the sender is following a leader already or thinks it is the leader
                    votes.put(en.getSenderID(), en);

                    if (en.getPeerEpoch() == this.proEpoch && haveEnoughVotes(votes, en)) {

                        return acceptElectionWinner(en);
                    } else {
                        if (en.getPeerEpoch() > this.proEpoch) {
                            if (haveEnoughVotes(votes, en)) {
                                return acceptElectionWinner(en);
                            }
                        }
                    }
                    break;
                case OBSERVER:
                    break;
                default:
                    break;
                // if no notifications received..
                // Remove next notification from queue, timing out after 2 times the termination
                // time
                // ..resend notifications to prompt a reply from others..
                // .and implement exponential back-off when notifications not received..
                // keep track of the votes I received and who I received them from.
                // if/when we get a message and it's from a valid server and for a valid
                // server..
                // switch on the state of the sender:
                // if the received message has a vote for a leader which supersedes mine, change
                // my vote and tell all my peers what my new vote is.
                //// if I have enough votes to declare my currently proposed leader as the
                //// leader:
                // first check if there are any new votes for a higher ranked possible leader
                // before I declare a leader. If so, continue in my election loop
                // If not, set my own state to either LEADING (if I won the election) or
                // FOLLOWING (if someone lese won the election) and exit the election
                // IF: see if the sender's vote allows me to reach a conclusion based on the
                // election epoch that I'm in, i.e. it gives the majority to the vote of the
                // FOLLOWING or LEADING peer whose vote I just received.
                // if so, accept the election winner.
                // As, once someone declares a winner, we are done. We are not worried about /
                // accounting for misbehaving peers.
                // ELSE: if n is from a LATER election epoch
                // IF a quorum from that epoch are voting for the same peer as the vote of the
                // FOLLOWING or LEADING peer whose vote I just received.
                // THEN accept their leader, and update my epoch to be their epoch
                // ELSE:
                // keep looping on the election loop.
            }
        }
        return getCurrentVote();
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        Vote vote = new Vote(n.getProposedLeaderID(), n.getPeerEpoch());
        this.proEpoch = n.getPeerEpoch();
        this.proLeader = n.getProposedLeaderID();
        if (vote.getProposedLeaderID() == myPeerServer.getServerId()) {
            myPeerServer.setPeerState(LEADING);
        } else {
            myPeerServer.setPeerState(FOLLOWING);
        }
        return vote;
        // set my state to either LEADING or FOLLOWING
        // clear out the incoming queue before returning
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proEpoch) || ((newEpoch == this.proEpoch) && (newId > this.proLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient
     * support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one
     * current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        int count = 0;
        if (this.proLeader == proposal.getProposedLeaderID()) {
            count++;
        }
        for (ElectionNotification n : votes.values()) {
            if (this.proEpoch != n.getPeerEpoch()) {
                continue;
            }
            if (proposal.getProposedLeaderID() == n.getProposedLeaderID()) {
                count++;
            }
            if (count > myPeerServer.getQuorumSize() / 2) {
                return true;
            }
        }
        return false;
    }

    static ElectionNotification getNotificationFromMessage(Message m) {
        if (m == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(m.getMessageContents());
        long pid = buffer.getLong();
        char s = buffer.getChar();
        long sid = buffer.getLong();
        long e = buffer.getLong();
        return new ElectionNotification(pid, ZooKeeperPeerServer.ServerState.getServerState(s), sid, e);
    }

    static byte[] buildMsgContent(ElectionNotification notification) {
        if (notification == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + Character.BYTES);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }

    private void sendNotifications() {
        byte[] m = buildMsgContent(new ElectionNotification(this.proLeader, this.myPeerServer.getPeerState(),
                this.myPeerServer.getServerId(), this.myPeerServer.getPeerEpoch()));
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, m);
    }

    private boolean isHigherVote() {
        LinkedList<Message> nonElectionQueue = new LinkedList<>();

        Message m = null;
        try {
            while ((m = this.incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                if(m.getMessageType()!= Message.MessageType.ELECTION){
                    nonElectionQueue.add(m);
                }
                ElectionNotification n = getNotificationFromMessage(m);
                if (this.proLeader < n.getProposedLeaderID()) {
                    this.incomingMessages.put(m);
                    this.incomingMessages.addAll(nonElectionQueue);
                    return true;
                } else {
                    m = null;
                }
            }
        } catch (InterruptedException e) {
        }
        this.incomingMessages.addAll(nonElectionQueue);
        return false;
    }
}
