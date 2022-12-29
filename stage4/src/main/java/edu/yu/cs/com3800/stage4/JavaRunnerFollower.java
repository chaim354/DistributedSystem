package edu.yu.cs.com3800.stage4;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage4.TCPServer.TcpMessage;;

public class JavaRunnerFollower extends Thread implements LoggingServer{
private int port;
private String host;
private Logger logger;
    public JavaRunnerFollower(int port,String host) {
        this.port = port+2;
        this.host = host;
        this.setDaemon(true);
    }

    @Override
    public void run() {
        ConcurrentLinkedQueue<TcpMessage> queue = new ConcurrentLinkedQueue<>();
        TCPServer server = new TCPServer(port, queue);
        server.start();
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName());
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
            ByteArrayInputStream inStream = new ByteArrayInputStream( tcpMessage.m.getMessageContents());
            JavaRunner jr = null;
            try {
                jr = new JavaRunner();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            String output = "n";
            try {
                output = jr.compileAndRun(inStream);
            } catch (Exception e) {

            }
            Message out = new Message(Message.MessageType.COMPLETED_WORK, output.getBytes(), tcpMessage.m.getReceiverHost(), tcpMessage.m.getReceiverPort(), tcpMessage.s.getInetAddress().getHostName(), tcpMessage.s.getPort(), tcpMessage.m.getRequestID());
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
