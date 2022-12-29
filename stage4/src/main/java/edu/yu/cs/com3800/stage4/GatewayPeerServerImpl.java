package edu.yu.cs.com3800.stage4;

import java.net.InetSocketAddress;
import java.util.Map;

import edu.yu.cs.com3800.ZooKeeperPeerServer;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl{

    public GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers);
        super.setPeerState(ZooKeeperPeerServer.ServerState.OBSERVER);
    }

    @Override
    public void setPeerState(ServerState newState) {
    }
    
}
