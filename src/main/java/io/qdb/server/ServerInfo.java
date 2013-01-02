package io.qdb.server;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.*;
import java.net.*;
import java.util.Enumeration;
import java.util.UUID;

/**
 * Publishes information about this server in ZooKeeper and discovers information about other servers in the same
 * cluster.
 */
public class ServerInfo {

/*
    private static final Logger log = LoggerFactory.getLogger(ServerInfo.class);

    private final JsonService jsonService;
    private final Zoo zoo;

    private final Info ourInfo = new Info();

    @Inject
    public ServerInfo(LocalStorage storage, JsonService jsonService, Zoo zoo,
                @Named("clusterName") String clusterName,
                @Named("ipAddress") String ipAddress,
                @Named("region") String region) throws IOException {
        this.jsonService = jsonService;
        this.zoo = zoo;

        File f = new File(storage.getDataDir(), "server-id.txt");
        if (f.exists()) {
            BufferedReader r = new BufferedReader(new FileReader(f));
            try {
                ourInfo.setId(r.readLine());
            } finally {
                r.close();
            }
        } else {
            ourInfo.setId(UUID.randomUUID().toString());
            FileWriter w = new FileWriter(f);
            try {
                w.write(ourInfo.getId());
            } finally {
                w.close();
            }
            log.info("Generated new server ID: " + ourInfo.getId());
        }

        if (ipAddress.length() == 0) {
            ipAddress = getFirstNonLoopbackAddress(true, false).toString();
        }

        ourInfo.setIpAddress(ipAddress);
        ourInfo.setRegion(region);

        log.info("This server " + ourInfo + " clusterName [" + clusterName + "]");
    }

    */
/**
     * Publish information about this server to ZooKeeper.
     *//*

    public void publish() throws IOException, InterruptedException, KeeperException {
        zoo.ensure("/nodes", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create("/nodes/" + ourInfo.getId(), jsonService.toJson(ourInfo), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    */
/**
     * From http://stackoverflow.com/questions/901755/how-to-get-the-ip-of-the-computer-on-linux-through-java
     *//*

    private static InetAddress getFirstNonLoopbackAddress(boolean preferIpv4, boolean preferIPv6) throws SocketException {
        Enumeration en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface i = (NetworkInterface) en.nextElement();
            for (Enumeration en2 = i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr = (InetAddress) en2.nextElement();
                if (!addr.isLoopbackAddress()) {
                    if (addr instanceof Inet4Address) {
                        if (preferIPv6) continue;
                        return addr;
                    }
                    if (addr instanceof Inet6Address) {
                        if (preferIpv4) continue;
                        return addr;
                    }
                }
            }
        }
        return null;
    }

    */
/**
     * Info on a server in our cluster.
     *//*

    public static class Info {

        @JsonIgnore
        private String id;
        private String ipAddress;
        private String region;

        public Info() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public void setIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        @Override
        public String toString() {
            return id + "," + ipAddress + ",region=" + region;
        }
    }
*/
}
