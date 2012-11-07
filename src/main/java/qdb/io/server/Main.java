package qdb.io.server;

import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Bootstraps the qdb server.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Container container = new HttpServer();
            Connection connection = new SocketConnection(container);
            SocketAddress address = new InetSocketAddress(8080);
            connection.connect(address);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

}
