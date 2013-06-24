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
