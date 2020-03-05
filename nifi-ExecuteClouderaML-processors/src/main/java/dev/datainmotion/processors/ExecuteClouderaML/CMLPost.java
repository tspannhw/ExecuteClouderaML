package dev.datainmotion.processors.ExecuteClouderaML;

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
        return "{\"accessKey\":\"" + accessKey +
                "\", \"request\": " + request + "}";
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", CMLPost.class.getSimpleName() + "[", "]" )
                .add( "accessKey='" + accessKey + "'" )
                .add( "request=" + request )
                .toString();
    }
}
