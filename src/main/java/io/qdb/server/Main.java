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

import ch.qos.logback.classic.Level;
import com.google.inject.*;
import io.qdb.server.output.OutputManager;
import org.simpleframework.transport.connect.Connection;
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

            // set logging level if we are using logback
            org.slf4j.Logger rootLogger = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
            if (rootLogger instanceof ch.qos.logback.classic.Logger) {
                String logLevel = System.getProperty("qdbLogLevel", mod.getLogLevel());
                ((ch.qos.logback.classic.Logger)rootLogger).setLevel(Level.toLevel(logLevel));
            }

            Injector injector = Guice.createInjector(mod);
            injector.getInstance(OutputManager.class);
            injector.getInstance(Connection.class);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
