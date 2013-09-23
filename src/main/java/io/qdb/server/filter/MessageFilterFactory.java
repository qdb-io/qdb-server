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
import io.qdb.server.controller.JsonService;
import io.qdb.server.databind.DataBinder;
import io.qdb.server.model.Queue;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;

/**
 * Creates {@link MessageFilter} instances.
 */
@Singleton
public class MessageFilterFactory {

    final Injector injector;
    final JsonService jsonService;

    @Inject
    public MessageFilterFactory(Injector injector, JsonService jsonService) {
        this.injector = injector;
        this.jsonService = jsonService;
    }

    /**
     * Create and initialize a MessageFilter instance. If filter is null then routingKey and grep are used to select
     * a filter if either or both are not null. Returns an 'accept all' filter if all 3 are null.
     * @throws IllegalArgumentException on invalid parameters or filter init failure
     */
    public MessageFilter createFilter(Map params, Queue q) throws IllegalArgumentException {
        String filter = (String)params.get("filter");
        if (filter == null || filter.length() == 0) {
            String routingKey = (String)params.get("routingKey");
            String grep = (String)params.get("grep");
            if (routingKey != null && routingKey.length() > 0) {
                if (grep != null && grep.length() > 0) filter = "standard";
                else filter = "routingKey";
            } else if (grep != null && grep.length() > 0) {
                filter = "grep";
            }
        }
        MessageFilter mf;
        if (filter != null) {
            mf = createFilter(filter);
            new DataBinder(jsonService).ignoreInvalidFields(true).bind(params, mf).check();
            mf.init(q);
        } else {
            mf = MessageFilter.NULL;
        }
        return mf;
    }

    /**
     * Create an MessageFilter instance for filter. Throws IllegalArgumentException if it is invalid or instance
     * creation fails. The filter parameter can be a built in short filter name (e.g. routingKey) or a fully qualified
     * class name.
     */
    @SuppressWarnings("unchecked")
    public MessageFilter createFilter(String filter) throws IllegalArgumentException {
        Class cls;
        if ("routingKey".equals(filter)) {
            cls = RoutingKeyMessageFilter.class;
        } else if ("grep".equals(filter)) {
            cls = GrepMessageFilter.class;
        } else if ("standard".equals(filter)) {
            cls = StandardMessageFilter.class;
        } else if (filter.contains(".")) {
            try {
                cls = Class.forName(filter);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Filter class not found [" + filter + "]");
            }
            if (!MessageFilter.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException("Filter [" + filter + "] does not implement " +
                        MessageFilter.class.getName());
            }
        } else {
            throw new IllegalArgumentException("Unknown filter [" + filter + "]");
        }
        try {
            return (MessageFilter)injector.getInstance(cls);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.toString(), e);
        }
    }

}
