package io.qdb.server.repo;

import com.google.common.io.ByteStreams;
import io.qdb.server.model.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Manages communication with a server in a QDB cluster over http.
 */
public class ClusterClient {

    private static final Logger log = LoggerFactory.getLogger(ClusterClient.class);

    public final Server server;

    private final JsonConverter jsonConverter;
    private final int timeoutMs;
    private final String authorization;

    @Singleton
    public static class Factory {

        private final JsonConverter jsonConverter;
        private final String clusterName;
        private final String clusterPassword;
        private final int clusterTimeoutMs;

        @Inject
        public Factory(JsonConverter jsonConverter,
                   @Named("clusterName") String clusterName,
                   @Named("clusterPassword") String clusterPassword,
                   @Named("clusterTimeoutMs") int clusterTimeoutMs) {
            this.jsonConverter = jsonConverter;
            this.clusterName = clusterName;
            this.clusterPassword = clusterPassword;
            this.clusterTimeoutMs = clusterTimeoutMs;
        }

        public ClusterClient create(Server server) {
            return new ClusterClient(jsonConverter, server, clusterName, clusterPassword, clusterTimeoutMs);
        }
    }

    public ClusterClient(JsonConverter jsonConverter, Server server, String username, String password, int timeoutMs) {
        this.jsonConverter = jsonConverter;
        this.server = server;
        this.timeoutMs = timeoutMs;
        try {
            this.authorization = "Basic " + DatatypeConverter.printBase64Binary((username + ":" + password).getBytes("UTF8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);  // not possible really
        }
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * GET from path. Convert the JSON response to an object of cls.
     * @exception ResponseCodeException if the response code is not 200
     */
    public <T> T GET(String path, Class<T> response) throws IOException {
        return call("GET", path, null, response, 200);
    }

    /**
     * Convert data to JSON and POST to path. Convert the JSON response to an object of cls.
     * @exception ResponseCodeException if the response code is not 201
     */
    public <T> T POST(String path, Object data, Class<T> response) throws IOException {
        return call("POST", path, data, response, 201);
    }

    private <T> T call(String method, String path, Object data, Class<T> response, int expectedCode) throws IOException {
        URL url = new URL(server.getId() + path);
        if (log.isDebugEnabled()) log.debug(method  + " " + url + (data != null ? " " + data : ""));

        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setConnectTimeout(timeoutMs);
        con.setReadTimeout(timeoutMs);
        con.setRequestProperty("Authorization", authorization);

        if (data != null) {
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);
            jsonConverter.writeValue(con.getOutputStream(), data);
        }

        int rc = con.getResponseCode();
        if (rc == expectedCode) {
            return jsonConverter.readValue(con.getInputStream(), response);
        } else {
            // always read the error stream so the underlying TCP connection can be re-used
            String text = null;
            try {
                InputStream es = con.getErrorStream();
                if (es != null) {
                    byte[] bytes = ByteStreams.toByteArray(es);
                    es.close();
                    text = new String(bytes, "UTF8");
                }
            } catch (Exception e) {
                log.error("Error reading response from ErrorStream: " + e);
            }
            throw new ResponseCodeException(method + " " + url + (data != null ? " " + data : "") + " returned " + rc +
                    (text == null ? "" : ": " + text), rc, text);
        }
    }

    @Override
    public String toString() {
        return server.toString();
    }
}
