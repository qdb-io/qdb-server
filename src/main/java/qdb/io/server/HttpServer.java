package qdb.io.server;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides a HTTP interface to qdb message queues.
 */
public class HttpServer implements Container {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    @Override
    public void handle(Request req, Response resp) {
        log.debug("path = " + req.getPath());
        resp.setCode(200);
        try {
            resp.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
