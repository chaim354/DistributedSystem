package edu.yu.cs.com3800.stage3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;;

public class JavaRunnerFollower extends Thread implements LoggingServer{
LinkedBlockingQueue<Message> incomingMessages;
LinkedBlockingQueue<Message> outgoingMessages;
private Logger logger;
    public JavaRunnerFollower(LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages) {
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.setDaemon(true);


    }

    @Override
    public void run() {
        int counter = 0;
        while (!this.isInterrupted()) {
            try {
                if(this.logger == null){
                    this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName());
                }
            } catch (IOException e) {
                this.logger.log(Level.WARNING,"failed to send packet", e);
            }
            Message message = null;
            while (message == null) {
                try {
                    message = incomingMessages.take();
                } catch (Exception e) {
                }
            }
            counter++;

            ByteArrayInputStream inStream = new ByteArrayInputStream( message.getMessageContents());
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
            outgoingMessages.add(new Message(Message.MessageType.COMPLETED_WORK, output.getBytes(), message.getReceiverHost(), message.getReceiverPort(), message.getSenderHost(), message.getSenderPort(),message.getRequestID()));

        }
    }

    public void shutdown() {
        interrupt();
    }
    
}
