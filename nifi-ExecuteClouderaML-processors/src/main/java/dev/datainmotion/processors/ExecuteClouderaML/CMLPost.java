package dev.datainmotion.processors.ExecuteClouderaML;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 *
 */
public class CMLPost implements Serializable
{
    private String accessKey;
    private String request;
    private final static long serialVersionUID = 106266571020743124L;

    public CMLPost() {
        super();
    }

    public CMLPost(String accessKey, String request) {
        super();
        this.accessKey = accessKey;
        this.request = request;
    }

    public String getAccessKey() {
        return accessKey;
    }
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getRequest() {
        return request;
    }
    public void setRequest(String request) {
        this.request = request;
    }

     // CML format hack
    public String getAsJSON() {
        Gson gson = new Gson();

        CMLCall call = new CMLCall();
        call.setAccessKey( this.accessKey );
        Request request = new Request();
        request.setSentence( this.request );
        call.setRequest( request );

        System.out.println(gson.toJson( call ));
        return gson.toJson(call);
//        return "{\"accessKey\":\"" + accessKey +
//                "\", \"request\": " + request + "}";
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", CMLPost.class.getSimpleName() + "[", "]" )
                .add( "accessKey='" + accessKey + "'" )
                .add( "request=" + request )
                .toString();
    }
}
