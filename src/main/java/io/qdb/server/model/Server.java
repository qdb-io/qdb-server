package io.qdb.server.model;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * A qdb instance in our cluster. The id of the server is its URL.
 */
public class Server extends ModelObject {

    public Server() { }

    public Server(String id) {
        setId(cleanURL(id));
    }

    @Override
    public String toString() {
        String id = getId();
        return id == null ? super.toString() : id;
    }

    /**
     * Create a URL to the server with a trailing slash.
     */
    public static String toURL(boolean https, String host, int port) {
        if (port <= 0) port = https ? 443 : 80;
        return (https ? "https" : "http") + "://" + host +
                (https && port == 443 || https && port == 80 ? "" : ":" + port) + "/";
    }

    /**
     * Cleanup a URL to one of our servers, validating it.
     */
    public static String cleanURL(String url) throws IllegalArgumentException {
        try {
            URL u = new URL(url);
            String protocol = u.getProtocol();
            boolean https;
            if ("https".equals(protocol)) https = true;
            else if ("http".equals(protocol)) https = false;
            else throw new IllegalArgumentException("Invalid protocol [" + protocol + "] in [" + url + "]");
            return toURL(https, u.getHost(), u.getPort());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed URL [" + url + "]");
        }
    }
}
