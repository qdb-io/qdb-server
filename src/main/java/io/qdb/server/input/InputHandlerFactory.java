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

package io.qdb.server.input;

import com.google.inject.Injector;
import io.qdb.server.output.OutputHandler;
import io.qdb.server.output.RabbitMQOutputHandler;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Creates {@link InputHandler} instances.
 */
@Singleton
public class InputHandlerFactory {

    final Injector injector;

    @Inject
    public InputHandlerFactory(Injector injector) {
        this.injector = injector;
    }

    /**
     * Create an InputHandler instance for type. Throws IllegalArgumentException if it is invalid or instance
     * creation fails. The type parameter can be a built in short type name (e.g. rabbitmq) or a fully qualified
     * class name.
     */
    @SuppressWarnings("unchecked")
    public InputHandler createHandler(String type) throws IllegalArgumentException {
        Class cls;
        if ("rabbitmq".equals(type)) {
            cls = RabbitMQInputHandler.class;
        } else if (type.contains(".")) {
            try {
                cls = Class.forName(type);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Input type class not found [" + type + "]");
            }
            if (!InputHandler.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException("Input type [" + type + "] does not extend " +
                        InputHandler.class.getName());
            }
        } else {
            throw new IllegalArgumentException("Unknown input type [" + type + "]");
        }
        try {
            return (InputHandler)injector.getInstance(cls);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.toString(), e);
        }
    }

}
