package io.qdb.server.repo;

import com.google.common.io.ByteStreams;
import io.qdb.server.model.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Manages communication with a QDB server over http.
 */
public class ClusterClient {

    private static final Logger log = LoggerFactory.getLogger(ClusterClient.class);

    private final JsonConverter jsonConverter;
    private final Server server;
    private final int timeoutMs;
    private final String authorization;

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

    public Server getServer() {
        return server;
    }

    /**
     * Convert data to JSON and POST to path. Convert the JSON response to an object of cls.
     * @exception ResponseCodeException if the response code is not 200 or 201
     */
    public <T> T POST(String path, Object data, Class<T> response) throws IOException {
        return call("POST", path, data, response);
    }

    private <T> T call(String method, String path, Object data, Class<T> response) throws IOException {
        URL url = new URL(server.getId() + path);
        if (log.isDebugEnabled()) log.debug("POST " + url + " " + data);

        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setConnectTimeout(timeoutMs);
        con.setReadTimeout(timeoutMs);
        con.setRequestProperty("Authorization", authorization);
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);

        jsonConverter.writeValue(con.getOutputStream(), data);

        int rc = con.getResponseCode();
        if (rc == 200 || rc == 201) {
            return jsonConverter.readValue(con.getInputStream(), response);
        } else {
            // always read the error stream so the underlying TCP connection can be re-used
            String text = null;
            try {
                InputStream es = con.getErrorStream();
                byte[] bytes = ByteStreams.toByteArray(es);
                es.close();
                text = new String(bytes, "UTF8");
            } catch (Exception e) {
                log.error("Error reading response from ErrorStream: " + e);
            }
            throw new ResponseCodeException("POST " + url + " " + data + " returned " + 500 +
                    (text == null ? "" : ": " + text), rc, text);
        }
    }

    @Override
    public String toString() {
        return server.toString();
    }
}
