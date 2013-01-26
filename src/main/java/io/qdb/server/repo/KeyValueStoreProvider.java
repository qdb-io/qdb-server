package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.KeyValueStoreBuilder;
import io.qdb.server.model.ModelObject;

import javax.inject.Inject;
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

    private final KeyValueStore<String, ModelObject> store;

    @SuppressWarnings("unchecked")
    @Inject
    public KeyValueStoreProvider(JsonSerializer jsonSerializer, VersionProvider versionProvider,
                KeyValueStoreListener listener,
                @Named("dataDir") String dataDir,
                @Named("txLogSizeM") int txLogSizeM,
                @Named("snapshotCount") int snapshotCount,
                @Named("snapshotIntervalSecs") int snapshotIntervalSecs)
            throws IOException {
        store = new KeyValueStoreBuilder<String, ModelObject>()
                .dir(new File(dataDir, "meta-data"))
                .serializer(jsonSerializer)
                .versionProvider(versionProvider)
                .listener(listener)
                .txLogSizeM(txLogSizeM)
                .snapshotCount(snapshotCount)
                .snapshotIntervalSecs(snapshotIntervalSecs)
                .create();
    }

    @Override
    public KeyValueStore<String, ModelObject> get() {
        return store;
    }
}
