package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.server.model.ModelObject;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Publishes events from our meta data {@link KeyValueStore} on the shared event bus.
 */
@Singleton
public class KeyValueStoreListener implements KeyValueStore.Listener<String, ModelObject> {

    private final EventBus eventBus;

    @Inject
    public KeyValueStoreListener(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void onKeyValueStoreEvent(KeyValueStore.Event<String, ModelObject> ev) {
        eventBus.post(ev);
    }
}
