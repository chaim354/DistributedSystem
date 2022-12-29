package edu.yu.cs.com3800.stage1;
import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.util.*;
public class ClientImpl implements Client {
    URI uri;
    private final HttpClient httpClient = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_2)
    .build();
    HttpResponse<String> response;
    public ClientImpl(String hostName, int hostPort) throws MalformedURLException{
    try {
        this.uri = new URI("http", null, hostName, hostPort, "/compileandrun", null, null);
    } catch (URISyntaxException e) {
        e.printStackTrace();
    }
    }

    public void sendCompileAndRunRequest(String src) throws IOException{
 
         HttpRequest request = HttpRequest.newBuilder()
                 .POST(BodyPublishers.ofString(src))
                 .uri(uri)
                 .header("Content-Type", "text/x-java-source")
                 .build();
 
        response = null;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    } 
    public Response getResponse() throws IOException{
        Response output = new Response(response.statusCode(), response.body());
        return output;
    }
}
