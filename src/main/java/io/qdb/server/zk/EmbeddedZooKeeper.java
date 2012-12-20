package io.qdb.server.zk;

import com.google.common.io.Files;
import io.qdb.server.ServerId;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * In-process ZooKeeper server for simple deployment and testing.
 */
@Singleton
public class EmbeddedZooKeeper implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedZooKeeper.class);

    private ZooKeeperServer zkServer;

    @Inject
    public EmbeddedZooKeeper(ServerId serverId,
            @Named("data.dir") String dataDir,
            @Named("zookeeper.connectString") String connectString,
            @Named("zookeeper.embedded.instance") int instance
            ) throws IOException {

        if (instance < 0) return; // embedding has been disabled

        String[] instances = connectString.split("[\\s]*,[\\s]*");
        if (instance >= instances.length) {
            throw new IOException("zookeeper.embedded.instance " + instance + " not in zookeeper.connectString [" +
                    connectString + "]");
        }

        String host;
        int clientPort;
        String addr = instances[instance];
        int i = addr.indexOf(':');
        if (i < 0) {
            host = addr;
            clientPort = 2181;
        } else {
            host = addr.substring(0, i);
            int j = addr.indexOf('/', i + 1);
            String portStr = j < 0 ? addr.substring(i + 1) : addr.substring(i + 1, j);
            try {
                clientPort = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port number [" + portStr + "] in instance " + instance +
                        " from zookeeper.connectString [" + connectString + "]");
            }
        }

        File zkDataDir = new File(dataDir, "zookeeper");
        if (!zkDataDir.exists()) {
            if (!zkDataDir.mkdirs()) {
                throw new IOException("Unable to create [" + zkDataDir.getAbsolutePath() + "]");
            }
        } else if (!zkDataDir.isDirectory()) {
            throw new IOException("Not a directory [" + zkDataDir.getAbsolutePath() + "]");
        }
        if (!zkDataDir.canWrite()) {
            throw new IOException("Unable to write to [" + zkDataDir.getAbsolutePath() + "]");
        }
        Files.write(serverId.get().getBytes("UTF8"), new File(zkDataDir, "myid"));

        Properties props = new Properties();
        props.setProperty("initLimit", "10");
        props.setProperty("syncLimit", "5");
        props.setProperty("dataDir", zkDataDir.getCanonicalPath());
        props.setProperty("clientPort", Integer.toString(clientPort));

        log.info("Starting ZooKeeper instance listening on " + host + ":" + clientPort);

        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (QuorumPeerConfig.ConfigException e) {
            throw new IOException(e);
        }

        // cut and paste from org.apache.zookeeper.server.ZooKeeperServerMain
        zkServer = new ZooKeeperServer();

        FileTxnSnapLog ftxn = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));
        zkServer.setTxnLogFactory(ftxn);
        zkServer.setTickTime(config.getTickTime());
        zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
        zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
        ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
        cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
        try {
            cnxnFactory.startup(zkServer);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (zkServer != null && zkServer.isRunning()) {
            zkServer.shutdown();
        }
    }
}
