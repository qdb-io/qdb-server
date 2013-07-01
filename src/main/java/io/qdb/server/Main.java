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

import com.google.inject.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bootstraps the qdb server.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            QdbServerModule mod = new QdbServerModule();
            new LogbackLogging().init(mod.getCfg());

            final ShutdownManager sm = Guice.createInjector(mod).getInstance(ShutdownManager.class);
            Runtime.getRuntime().addShutdownHook(new Thread("qdb-shutdown-hook") {
                @Override
                public void run() { sm.close(); }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(1);
        }
    }

}
