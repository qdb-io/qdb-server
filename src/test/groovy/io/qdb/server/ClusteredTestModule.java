package io.qdb.server;

import com.google.inject.Key;
import com.google.inject.name.Names;

import java.io.File;

/**
 * Server configured for clustered testing.
 */
public class ClusteredTestModule extends StandaloneTestModule {

    private final int instance;

    public ClusteredTestModule(File dataDir, int instance) {
        super(dataDir);
        this.instance = instance;
    }

    @Override
    protected void configure() {
        super.configure();
        bind(Key.get(String.class, Names.named("zookeeper.connectString"))).toInstance(
                "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");
        bind(Key.get(Integer.class, Names.named("zookeeper.instance"))).toInstance(instance);
        bind(Key.get(Integer.class, Names.named("port"))).toInstance(9554 + instance);
    }
}
