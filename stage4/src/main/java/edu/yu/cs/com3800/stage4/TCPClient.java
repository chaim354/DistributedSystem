package edu.yu.cs.com3800.stage4;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage4.TCPServer.TcpMessage;

public class TCPClient extends Thread{
    private TcpMessage tcpMessage;
    private Socket socket;
    private int port;
    private String host;
    public TCPClient(int port,String host, TcpMessage tcpMessage) {

        this.tcpMessage = tcpMessage;
        this.port = port;
        this.host = host;
        setDaemon(true);
    }

    @Override
    public void run(){
        try {
            socket =  new  Socket(host, port);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Message m = new Message(Message.MessageType.WORK, tcpMessage.m.getMessageContents(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(),host, port, tcpMessage.m.getRequestID());
        try {
            socket.getOutputStream().write(m.getNetworkPayload());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        byte[] o = null;
        try {
            o = Util.readAllBytesFromNetwork(socket.getInputStream());
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {

            socket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        Message n = new Message(o);
        Message out = new Message(Message.MessageType.COMPLETED_WORK, n.getMessageContents(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(), tcpMessage.s.getInetAddress().getHostName(), tcpMessage.s.getPort(), tcpMessage.m.getRequestID());
        try {
            tcpMessage.s.getOutputStream().write(out.getNetworkPayload());
            tcpMessage.s.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    
}
