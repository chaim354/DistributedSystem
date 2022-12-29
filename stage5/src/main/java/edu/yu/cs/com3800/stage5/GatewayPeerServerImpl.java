package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import edu.yu.cs.com3800.ZooKeeperPeerServer;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl{

    public GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, ConcurrentMap<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers);
        super.setPeerState(ZooKeeperPeerServer.ServerState.OBSERVER);
    }

    @Override
    public void setPeerState(ServerState newState) {
    }
    
}
