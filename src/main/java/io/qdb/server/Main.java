package io.qdb.server;

import com.google.inject.*;
import com.typesafe.config.Config;
import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Bootstraps the qdb server.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Injector injector = Guice.createInjector(new StdModule());
//            ZooKeeperConnector zoo = injector.getInstance(ZooKeeperConnector.class);
//            zoo.ensureConnected();
//
//            for (;;) {
//                Thread.sleep(1000);
//            }

            Config cfg = injector.getInstance(Config.class);
            Container container = injector.getInstance(Container.class);
            Connection connection = new SocketConnection(container);
            SocketAddress address = new InetSocketAddress(cfg.getString("host"), cfg.getInt("port"));
            log.info("Listening on " + address);
            connection.connect(address);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
