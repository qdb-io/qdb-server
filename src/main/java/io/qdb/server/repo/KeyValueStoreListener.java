/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.server.model.ModelObject;

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
