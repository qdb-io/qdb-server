package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.server.model.ModelObject;
import io.qdb.server.model.Repository;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Converts events from our meta data {@link KeyValueStore} to {@link Repository.ObjectEvent}'s and publishes
 * them on the shared event bus.
 */
@Singleton
public class KeyValueStoreListener implements KeyValueStore.Listener<String, ModelObject> {

    private final EventBus eventBus;

    @Inject
    public KeyValueStoreListener(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public void onObjectEvent(KeyValueStore.ObjectEvent<String, ModelObject> ev) {
        Repository.ObjectEvent.Type type;
        switch (ev.type) {
            case CREATED:   type = Repository.ObjectEvent.Type.CREATED;     break;
            case UPDATED:   type = Repository.ObjectEvent.Type.UPDATED;     break;
            case DELETED:   type = Repository.ObjectEvent.Type.DELETED;     break;
            default:        return;
        }
        eventBus.post(new Repository.ObjectEvent(type, ev.value));
    }
}
