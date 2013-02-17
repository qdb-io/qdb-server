package io.qdb.server.repo.cluster;

import com.google.common.io.ByteStreams;
import io.qdb.server.OurServer;
import io.qdb.server.model.Repository;
import io.qdb.server.model.Server;
import io.qdb.server.repo.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.xml.bind.DatatypeConverter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Manages communication with a server in a QDB cluster over http.
 */
public class ClusterClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ClusterClient.class);

    public final Server server;

    private final String referer;
    private final JsonSerializer jsonConverter;
    private final int timeoutMs;
    private final String authorization;

    private long lastContact;

    @Singleton
    public static class Factory {

        private final OurServer ourServer;
        private final JsonSerializer jsonConverter;
        private final String clusterName;
        private final String clusterPassword;
        private final int clusterTimeoutMs;

        @Inject
        public Factory(OurServer ourServer, JsonSerializer jsonConverter,
                   @Named("clusterName") String clusterName,
                   @Named("clusterPassword") String clusterPassword,
                   @Named("clusterTimeoutMs") int clusterTimeoutMs) {
            this.ourServer = ourServer;
            this.jsonConverter = jsonConverter;
            this.clusterName = clusterName;
            this.clusterPassword = clusterPassword;
            this.clusterTimeoutMs = clusterTimeoutMs;
        }

        public ClusterClient create(Server server) {
            return new ClusterClient(ourServer, jsonConverter, server, clusterName, clusterPassword, clusterTimeoutMs);
        }
    }

    public ClusterClient(OurServer ourServer, JsonSerializer jsonConverter, Server server, String username,
                String password, int timeoutMs) {
        this.referer = ourServer.getId();
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

    public long getLastContact() {
        return lastContact;
    }

    public void updateLastContact() {
        this.lastContact = System.currentTimeMillis();
    }

    public Repository.ServerStatus getStatus() {
        return new Repository.ServerStatus(server.getId(), null,
                lastContact > 0 ? (int)(System.currentTimeMillis() - lastContact) : null, null);
    }

    /**
     * Send msg to the endpoint at path and return the response data (or empty array if none).
     *
     * @exception IOException if the response code is not 201
     */
    public byte[] send(String path, byte[] msg) throws IOException {
        URL url = new URL(server.getId() + path);
        if (log.isDebugEnabled()) log.debug("POST " + url + " " + msg.length + " byte(s)");

        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("POST");
        con.setConnectTimeout(timeoutMs);
        con.setReadTimeout(timeoutMs);
        con.setRequestProperty("Authorization", authorization);
        con.setRequestProperty("Referer", referer);

        con.setRequestProperty("Content-Type", "application/octet-stream");
        con.setDoOutput(true);
        jsonConverter.writeValue(con.getOutputStream(), msg);

        int rc = con.getResponseCode();
        if (rc == 200 || rc == 201) {
            updateLastContact();
            InputStream ins = con.getInputStream();
            try {
                return ByteStreams.toByteArray(ins);
            } finally {
                close(ins);
            }
        } else {
            // always read the error stream so the underlying TCP connection can be re-used
            String text = null;
            try {
                InputStream es = con.getErrorStream();
                if (es != null) {
                    byte[] bytes = ByteStreams.toByteArray(es);
                    es.close();
                    // assume the response is UTF8 text for an error of some kind
                    text = new String(bytes, "UTF8");
                }
            } catch (Exception e) {
                log.error("Error reading response from ErrorStream: " + e);
            }
            throw new IOException("POST " + url + " " + msg.length + " byte(s) returned " + rc +
                    (text == null ? "" : ": " + text));
        }
    }

    @Override
    public String toString() {
        return server.toString();
    }

    private void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                log.debug("Error closing " + c + ": " + e.toString());
            }
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to do yet
    }
}
