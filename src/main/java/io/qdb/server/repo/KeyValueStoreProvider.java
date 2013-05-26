package io.qdb.server.repo;

import com.google.inject.Inject;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.KeyValueStoreBuilder;
import io.qdb.server.model.Database;
import io.qdb.server.model.ModelObject;
import io.qdb.server.model.Queue;
import io.qdb.server.model.User;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;

/**
 * Creates the key value store for server meta data.
 */
@Singleton
public class KeyValueStoreProvider implements Provider<KeyValueStore<String, ModelObject>> {

    private final VersionProvider versionProvider;
    private final KeyValueStoreListener listener;
    private final String dataDir;
    private final int txLogSizeM;
    private final int snapshotCount;
    private final int snapshotIntervalSecs;

    private KeyValueStore<String, ModelObject> store;

    @Inject
    public KeyValueStoreProvider(VersionProvider versionProvider, KeyValueStoreListener listener,
                @Named("dataDir") String dataDir,
                @Named("txLogSizeM") int txLogSizeM,
                @Named("snapshotCount") int snapshotCount,
                @Named("snapshotIntervalSecs") int snapshotIntervalSecs) {
        this.versionProvider = versionProvider;
        this.listener = listener;
        this.dataDir = dataDir;
        this.txLogSizeM = txLogSizeM;
        this.snapshotCount = snapshotCount;
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

    @Override
    @SuppressWarnings("unchecked")
    public KeyValueStore<String, ModelObject> get() {
        if (store == null) {
            try {
                store = new KeyValueStoreBuilder<String, ModelObject>()
                        .dir(new File(dataDir, "meta-data"))
                        .versionProvider(versionProvider)
                        .listener(listener)
                        .txLogSizeM(txLogSizeM)
                        .snapshotCount(snapshotCount)
                        .snapshotIntervalSecs(snapshotIntervalSecs)
                        .alias("user", User.class)
                        .alias("database", Database.class)
                        .alias("queue", Queue.class)
                        .create();
            } catch (IOException e) {
                throw new IllegalStateException(e.toString(), e);
            }
        }
        return store;
    }
}
