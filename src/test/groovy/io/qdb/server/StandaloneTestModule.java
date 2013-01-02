package io.qdb.server;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;

import java.io.File;

/**
 * Server configured for testing.
 */
public class StandaloneTestModule extends AbstractModule {

    private final File dataDir;

    public StandaloneTestModule(File dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    protected void configure() {
        bind(Key.get(String.class, Names.named("dataDir"))).toInstance(dataDir.getAbsolutePath());
    }
}
