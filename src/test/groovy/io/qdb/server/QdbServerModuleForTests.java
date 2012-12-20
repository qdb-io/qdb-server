package io.qdb.server;

import com.google.inject.Key;
import com.google.inject.name.Names;

/**
 * Server configured for testing.
 */
public class QdbServerModuleForTests extends QdbServerModule {

    public QdbServerModuleForTests(String dataDir) {
    }

    @Override
    protected void bindProperties() {
        super.bindProperties();

//        bind(Key.get(String.class, Names.named("zookeeper.connectString"))).toInstance(zkConnectString);
    }
}
