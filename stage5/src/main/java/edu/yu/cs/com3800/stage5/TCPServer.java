package edu.yu.cs.com3800.stage5;


import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import edu.yu.cs.com3800.*;
public class TCPServer extends Thread{
    private ConcurrentLinkedQueue<TcpMessage> queue;
    private int port;
    public TCPServer(int port, ConcurrentLinkedQueue<TcpMessage> queue) {
        this.queue = queue;
        this.port = port;
        setDaemon(true);
    }

    @Override
    public void run(){

        ServerSocket serverSocket;

        try {
            serverSocket = new ServerSocket(port);
            Socket socket;
            while ((socket = serverSocket.accept())!=null) {
                
                Byte b;
                List<Byte> output = new ArrayList<>();
                byte[] temp = Util.readAllBytesFromNetwork(socket.getInputStream());
               Message  out = new Message(temp);
                this.queue.offer(new TcpMessage(socket, out));
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public ConcurrentLinkedQueue<TcpMessage> getQueue(){
        return queue;
    }
    class TcpMessage{
         Socket s;
         Message m;
    public TcpMessage(Socket s,Message m){
        this.s =s;
        this.m = m;
    }         
    }

}
