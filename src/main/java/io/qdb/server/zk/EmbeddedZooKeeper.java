package io.qdb.server.zk;

import com.google.common.io.Files;
import io.qdb.server.ServerId;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
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

    private ServerCnxnFactory cnxnFactory;
    private QuorumPeer quorumPeer;

    private static final String COMMA_LIST_REGEX = "[\\s]*,[\\s]*";

    @Inject
    public EmbeddedZooKeeper(
            @Named("data.dir") String dataDir,
            @Named("zookeeper.connectString") String connectString,
            @Named("zookeeper.instance") int instance,
            @Named("zookeeper.servers") String servers
            ) throws IOException {

        if (instance < 1) return; // embedding has been disabled

        String[] instances = connectString.split(COMMA_LIST_REGEX);
        if (instance > instances.length) {
            throw new IOException("zookeeper.instance " + instance + " not in zookeeper.connectString [" +
                    connectString + "]");
        }

        String host;
        int clientPort;
        String addr = instances[instance - 1];
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
        Files.write((instance + "\n").getBytes("UTF8"), new File(zkDataDir, "myid"));

        Properties props = new Properties();
        props.setProperty("initLimit", "10");
        props.setProperty("syncLimit", "5");
        props.setProperty("dataDir", zkDataDir.getCanonicalPath());
        props.setProperty("clientPort", Integer.toString(clientPort));

        if (instances.length <= 1) {
            startStandalone(host, clientPort, props);
            return;
        }

        String[] zkServers = servers.split(COMMA_LIST_REGEX);
        if (zkServers.length != instances.length) {
            throw new IllegalArgumentException("The zookeeper.servers list [" + servers +  "] must have the same " +
                    "number of entries as zookeeper.connectString [" + connectString + "]");
        }
        for (i = 0; i < zkServers.length; i++) props.setProperty("server." + (i + 1), zkServers[i]);

        startClustered(host, clientPort, props, instance);
    }

    private void startStandalone(String host, int clientPort, Properties props) throws IOException {
        log.info("Starting standalone ZooKeeper instance listening on " + host + ":" + clientPort);

        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (QuorumPeerConfig.ConfigException e) {
            throw new IOException(e);
        }

        // cut and paste from org.apache.zookeeper.server.ZooKeeperServerMain
        ZooKeeperServer zkServer = new ZooKeeperServer();

        FileTxnSnapLog ftxn = new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir()));
        zkServer.setTxnLogFactory(ftxn);
        zkServer.setTickTime(config.getTickTime());
        zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
        zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());

        cnxnFactory = createCnxnFactory(config);
        try {
            cnxnFactory.startup(zkServer);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private ServerCnxnFactory createCnxnFactory(QuorumPeerConfig config) throws IOException {
        if (System.getProperty("zookeeper.serverCnxnFactory") == null) {
            System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        }
        ServerCnxnFactory f = ServerCnxnFactory.createFactory();
        f.configure(config.getClientPortAddress(), config.getMaxClientCnxns());
        return f;
    }

    private void startClustered(String host, int clientPort, Properties props, int instance) throws IOException {
        log.info("Starting clustered ZooKeeper instance listening on " + host + ":" + clientPort);

        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parseProperties(props);
        } catch (QuorumPeerConfig.ConfigException e) {
            throw new IOException(e);
        }

        ServerCnxnFactory cnxnFactory = createCnxnFactory(config);

        QuorumPeer quorumPeer = new QuorumPeer();
        quorumPeer.setClientPortAddress(config.getClientPortAddress());
        quorumPeer.setTxnFactory(new FileTxnSnapLog(
                new File(config.getDataLogDir()),
                new File(config.getDataDir())));
        quorumPeer.setQuorumPeers(config.getServers());
        quorumPeer.setElectionType(config.getElectionAlg());
        quorumPeer.setMyid(config.getServerId());
        quorumPeer.setTickTime(config.getTickTime());
        quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
        quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
        quorumPeer.setInitLimit(config.getInitLimit());
        quorumPeer.setSyncLimit(config.getSyncLimit());
        quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
        quorumPeer.setCnxnFactory(cnxnFactory);
        quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
        quorumPeer.setLearnerType(config.getPeerType());

        quorumPeer.start();
    }

    @Override
    public void close() throws IOException {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
            cnxnFactory = null;
        }
        if (quorumPeer != null) {
            quorumPeer.shutdown();
            quorumPeer = null;
        }
    }
}
