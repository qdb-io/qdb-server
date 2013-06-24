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

import io.qdb.server.databind.HasAnySetter;

import java.util.HashMap;
import java.util.Map;

/**
 * For {@link io.qdb.server.DataBinderSpec}.
 */
public class TypesDTOWithParams extends TypesDTO implements HasAnySetter {

    public transient Map<String, Object> params;

    @Override
    public void set(String key, Object value) {
        if (params == null) params = new HashMap<String, Object>();
        params.put(key, value);
    }

    @Override
    public Map<String, Object> getParams() {
        return params;
    }
}
