

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage5.*;
import edu.yu.cs.com3800.Client.Response;
import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class ServerRunner {
    private static int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070};
    //private int[] ports = {8010, 8020};
    private static int myPort = 9999;
    private static InetSocketAddress myAddress = new InetSocketAddress("localhost", myPort);
    public static void main(String[] args) {
            //create IDs and addresses
            ConcurrentMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(8);
            for (int i = 0; i < ports.length; i++) {
                peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", ports[i]));
            }
            long thisServer = Integer.valueOf(Integer.parseInt(args[0])).longValue();
            System.out.println(thisServer);
            //create servers
            if(thisServer==-1L){
                System.out.println("gateway Started before ");
                GatewayServer gs = new GatewayServer(8100, 0, -1L, peerIDtoAddress, 1);
                System.out.println("gateway Started");

            } else {
                ConcurrentHashMap<Long, InetSocketAddress> map = (ConcurrentHashMap<Long, InetSocketAddress>) peerIDtoAddress;
                InetSocketAddress t = map.remove(thisServer);
                map.put(-1L, new InetSocketAddress("localhost", 8100));
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(t.getPort(), 0, thisServer, map,-1L,1);
                server.start();
                System.out.println(thisServer + " server started");
            }
    }
}
