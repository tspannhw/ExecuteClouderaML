package dev.datainmotion.processors.ExecuteClouderaML;

public class CMLCall {

    private String accessKey;
    Request request;


    // Getter Methods

    public String getAccessKey() {
        return accessKey;
    }

    public Request getRequest() {
        return request;
    }

    // Setter Methods

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setRequest(Request r) {
        this.request = r;
    }
}