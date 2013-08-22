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

package io.qdb.server.filter;

import com.google.inject.Injector;
import io.qdb.server.output.OutputHandler;
import io.qdb.server.output.RabbitMQOutputHandler;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Creates {@link MessageFilter} instances.
 */
@Singleton
public class MessageFilterFactory {

    final Injector injector;

    @Inject
    public MessageFilterFactory(Injector injector) {
        this.injector = injector;
    }

    /**
     * Create an OutputHandler instance for type. Throws IllegalArgumentException if it is invalid or instance
     * creation fails. The type parameter can be a built in short type name (e.g. rabbitmq) or a fully qualified
     * class name.
     */
    @SuppressWarnings("unchecked")
    public MessageFilter createFilter(String type) throws IllegalArgumentException {
        Class cls;
        if ("routingKey".equals(type)) {
            cls = RoutingKeyMessageFilter.class;
        } else if (type.contains(".")) {
            try {
                cls = Class.forName(type);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Filter class not found [" + type + "]");
            }
            if (!MessageFilter.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException("Filter [" + type + "] does not implement " +
                        MessageFilter.class.getName());
            }
        } else {
            throw new IllegalArgumentException("Unknown filter [" + type + "]");
        }
        try {
            return (MessageFilter)injector.getInstance(cls);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.toString(), e);
        }
    }

}
