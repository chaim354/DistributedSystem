package edu.yu.cs.com3800.stage1;

import java.io.*;
import java.io.InputStream;
import java.net.InetSocketAddress;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import edu.yu.cs.com3800.*;

public class SimpleServerImpl implements SimpleServer{
    private int port;
    private HttpServer server;
    private static final Logger logger = Logger.getLogger(SimpleServerImpl.class.getName());

    private int requestCount;
    public static void main(String[] args) {
        int port = 9000; 
        if(args.length >0) {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try {
        myserver = new SimpleServerImpl(port);
        myserver.start();
        } catch(Exception e) {
        myserver.stop();
        } 
    }

    public SimpleServerImpl(int port) throws IOException{
        this.port = port;
        this.server = server.create(new InetSocketAddress(port), 0);
        server.createContext("/compileandrun", new MyHandler());
        FileHandler fh;  

    try {  

        // This block configure the logger with handler and formatter  
        fh = new FileHandler("./src/main/java/edu/yu/cs/com3800/logs/className.log");  
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter);  
        // the following statement is used to log any messages  
        logger.info("My first log");  

    } catch (SecurityException e) {  
        e.printStackTrace();  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  

    }

     /**
     * start the server
     */
    public void start(){
        logger.info("server starting");
        server.start();
    }

    /**
     * stop the server
     */
    public void stop(){
        logger.info("server stopping");
        server.stop(0);
    }
    


    class MyHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            requestCount++;
            logger.info(String.valueOf("request count: " + requestCount));
            Map headers = t.getRequestHeaders();
            if(!(headers.get("Content-Type").toString().equals("[text/x-java-source]"))){
                String response = "incorrect content type";
                t.sendResponseHeaders(400, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
                logger.warning("request had wrong content type");
                return;
            }
            InputStream is = t.getRequestBody();
            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            is.transferTo(bas);
            ByteArrayInputStream inStream = new ByteArrayInputStream( bas.toByteArray());
            JavaRunner jr = new JavaRunner();
            String output = "n";
            try {
                output = jr.compileAndRun(inStream);
            } catch (Exception e) {
                StringBuilder sb = new StringBuilder();
                sb.append(e.getMessage());
                sb.append("\n");
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream s = new PrintStream( baos );       
                e.printStackTrace(s);
                sb.append(baos.toString());
                String response = sb.toString();
                logger.warning("error encountered with request: " + String.valueOf(requestCount)+ " " +response);
                t.sendResponseHeaders(400, response.length());
                OutputStream os = t.getResponseBody();
                os.write(response.getBytes());
                os.close();
                logger.warning("code failed:" + response);
                return;
            }
            String response = output;
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
            logger.info("code response: " + response);
        }
    }
}
