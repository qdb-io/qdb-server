package io.qdb.server;

import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Creates our listener.
 */
@Singleton
public class ConnectionProvider implements Provider<Connection> {

    private static final Logger log = LoggerFactory.getLogger(ConnectionProvider.class);

    private final Container container;
    private final String host;
    private final int port;
    private final boolean https;

    private Connection connection;

    @Inject
    public ConnectionProvider(Container container,
                @Named("host") String host,
                @Named("port") int port,
                @Named("https") boolean https) {
        this.container = container;
        this.host = host;
        this.port = port;
        this.https = https;
    }

    @Override
    public Connection get() {
        if (https) throw new IllegalStateException("https = true not implemented");
        try {
            if (connection == null) {
                SocketAddress address = new InetSocketAddress(host, port);
                connection = new SocketConnection(container);
                connection.connect(address);
                log.info("QDB Server listening on " + host + ":" + port);
            }
            return connection;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
