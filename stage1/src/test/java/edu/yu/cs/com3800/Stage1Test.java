package edu.yu.cs.com3800;


import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.MalformedURLException;

import edu.yu.cs.com3800.stage1.Client;
import edu.yu.cs.com3800.stage1.ClientImpl;
import edu.yu.cs.com3800.stage1.SimpleServerImpl;
import edu.yu.cs.com3800.stage1.Client.Response;
import edu.yu.cs.com3800.*;
public class Stage1Test {

    @Test
    void testPostMultipleRequests() {
        int port = 9000; 
        SimpleServer myserver = null;
        try {
        myserver = new SimpleServerImpl(port);
        myserver.start();
        } catch(Exception e) {
        myserver.stop();
        } 
        for(int i = 0;i<100;i++){
        Client client = null;
        Response r = null;
        try {
            client = new ClientImpl("localhost", 9000);
            
            client.sendCompileAndRunRequest("public class Test {public Test(){}public String run() {return \"belisimo we did it\";}}");
            
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            r = client.getResponse();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertEquals( "belisimo we did it",r.getBody());
        assertEquals(200, r.getCode());
    }
    myserver.stop();
    }
    @Test
    void testOneRequest(){
        int port = 9000; 
        SimpleServer myserver = null;
        try {
        myserver = new SimpleServerImpl(port);
        myserver.start();
        } catch(Exception e) {
        myserver.stop();
        } 
        Client client = null;
        Response r = null;
        try {
            client = new ClientImpl("localhost", 9000);
            
            client.sendCompileAndRunRequest("public class Test {public Test(){}public String run() {return \"belisimo we did it\";}}");
            
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            r = client.getResponse();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        myserver.stop();
        assertEquals( "belisimo we did it",r.getBody());
        assertEquals(200, r.getCode());
    }

    @Test
    void testInvalidCodeAndPrintError(){
        int port = 9000; 
        SimpleServer myserver = null;
        try {
        myserver = new SimpleServerImpl(port);
        myserver.start();
        } catch(Exception e) {
        myserver.stop();
        } 
        Client client = null;
        Response r = null;
        try {
            client = new ClientImpl("localhost", 9000);
            
            client.sendCompileAndRunRequest("public class Test {public Test(){}public String rut() {return \"belisimo we did it\";}}");
            
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            r = client.getResponse();
        } catch (IOException e) {
            e.printStackTrace();
        }
        myserver.stop();
        //System.out.println(r.getBody());
        assertEquals(400, r.getCode());
        assertEquals(true, r.getBody().contains("java.lang.NoSuchMethodException: Test.run()"));
    }



}