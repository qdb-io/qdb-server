package io.qdb.server;

import io.qdb.server.model.Server;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Our server details derived from our configuration settings.
 */
@Singleton
public class OurServer extends Server {

    private final String host;
    private final int port;
    private final boolean https;

    @Inject
    public OurServer(@Named("host") String host, @Named("port") int port, @Named("https") boolean https) {
        this.host = host;
        this.port = port;
        this.https = https;
        setId(toURL(https, host, port));
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isHttps() {
        return https;
    }
}
