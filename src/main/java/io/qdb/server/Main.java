package io.qdb.server;

import com.google.inject.*;
import io.qdb.server.zk.EmbeddedZooKeeper;
import org.simpleframework.transport.connect.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstraps the qdb server.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Injector injector = Guice.createInjector(new QdbServerModule());
            injector.getInstance(Connection.class);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
