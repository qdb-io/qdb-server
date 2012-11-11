package qdb.io.server;

import com.google.inject.*;
import com.typesafe.config.Config;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.simpleframework.http.core.Container;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Bootstraps the qdb server.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Injector injector = Guice.createInjector(new StdModule());
            ZooKeeperConnector zoo = injector.getInstance(ZooKeeperConnector.class);
            zoo.ensureConnected();

//            ZooKeeper zk = new ZooKeeper("127.0.0.1", 2181, watcher);
//            zk.create("/oink", new byte[]{123}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            zk.exists("/oink", watcher);

            for (;;) {
                Thread.sleep(1000);
            }

//            Config cfg = injector.getInstance(Config.class);
//            Container container = injector.getInstance(Container.class);
//            Connection connection = new SocketConnection(container);
//            SocketAddress address = new InetSocketAddress(cfg.getString("host"), cfg.getInt("port"));
//            log.info("Listening on " + address);
//            connection.connect(address);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
