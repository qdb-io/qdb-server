package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.KeyValueStoreBuilder;
import io.qdb.kvstore.cluster.Transport;
import io.qdb.server.model.ModelObject;
import io.qdb.server.repo.cluster.ServerLocator;

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

    private final JsonSerializer jsonSerializer;
    private final VersionProvider versionProvider;
    private final KeyValueStoreListener listener;
    private final String dataDir;
    private final int txLogSizeM;
    private final int snapshotCount;
    private final int snapshotIntervalSecs;

    // this is only needed for clustered deployment
    @Inject(optional = true) private Transport transport;

    private KeyValueStore<String, ModelObject> store;

    @Inject
    public KeyValueStoreProvider(JsonSerializer jsonSerializer, VersionProvider versionProvider,
                KeyValueStoreListener listener,
                @Named("dataDir") String dataDir,
                @Named("txLogSizeM") int txLogSizeM,
                @Named("snapshotCount") int snapshotCount,
                @Named("snapshotIntervalSecs") int snapshotIntervalSecs) {
        this.jsonSerializer = jsonSerializer;
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
                        .serializer(jsonSerializer)
                        .versionProvider(versionProvider)
                        .listener(listener)
                        .txLogSizeM(txLogSizeM)
                        .snapshotCount(snapshotCount)
                        .snapshotIntervalSecs(snapshotIntervalSecs)
                        .transport(transport)
                        .create();
            } catch (IOException e) {
                throw new IllegalStateException(e.toString(), e);
            }
        }
        return store;
    }
}
