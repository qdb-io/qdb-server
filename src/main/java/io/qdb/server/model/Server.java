package io.qdb.server.model;

/**
 * A qdb instance in our cluster.
 */
public class Server extends ModelObject {

    private String url;

    public Server() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

}
