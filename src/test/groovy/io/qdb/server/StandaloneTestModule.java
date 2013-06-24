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
