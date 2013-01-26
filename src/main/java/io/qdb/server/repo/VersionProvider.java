package io.qdb.server.repo;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.server.model.ModelObject;

import javax.inject.Singleton;

/**
 * Access to version numbers of our model objects for {@link KeyValueStore}.
 */
@Singleton
public class VersionProvider implements KeyValueStore.VersionProvider<ModelObject> {

    @Override
    public Object getVersion(ModelObject value) {
        return value.getVersion();
    }

    @Override
    public void incVersion(ModelObject value) {
        value.incVersion();
    }
}
