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

package io.qdb.server.databind;

import io.qdb.server.controller.JsonService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds keys and values from a map to objects. Accumulates errors.
 */
public class DataBinder {

    private final JsonService jsonService;
    private boolean ignoreInvalidFields;
    private boolean updateMap;
    private Map<String, String> errors;

    public DataBinder(JsonService jsonService) {
        this.jsonService = jsonService;
    }

    public DataBinder ignoreInvalidFields(boolean on) {
        this.ignoreInvalidFields = on;
        return this;
    }

    /** If true then converted data from the map is put back in the map. */
    public DataBinder updateMap(boolean on) {
        this.updateMap = on;
        return this;
    }

    /**
     * Bind the values of map to matching public field names from dto. Support basic data types and does conversions
     * from String to these types. If dto implements {@link HasAnySetter} the keys from the map that do not match
     * field names are passed to {@link HasAnySetter#set(String, Object)}.
     */
    @SuppressWarnings("unchecked")
    public DataBinder bind(Map map, Object dto) {
        Class<?> cls = dto.getClass();
        for (Object o : map.entrySet()) {
            Map.Entry e = (Map.Entry)o;
            String key = (String)e.getKey();
            Object v = e.getValue();
            Field f;
            try {
                f = cls.getField(key);
            } catch (NoSuchFieldException x) {
                if (dto instanceof HasAnySetter) {
                    ((HasAnySetter)dto).set(key, v);
                } else if (!ignoreInvalidFields) {
                    error(key, "Unknown field");
                }
                continue;
            }
            Class t = f.getType();
            if (v instanceof String && t != String.class) {
                String s = (String)v;
                try {
                    if (t == Integer.TYPE || t == Integer.class) v = IntegerParser.INSTANCE.parseInt(s);
                    else if (t == Long.TYPE || t == Long.class) v = IntegerParser.INSTANCE.parseLong(s);
                    else if (t == Boolean.TYPE || t == Boolean.class) v = "true".equals(v);
                    else if (t == Date.class) v = DateTimeParser.INSTANCE.parse(s);
                    else if (t == String[].class) v = parseStringArray(s);
                } catch (Exception x) {
                    error(key, "Invalid value, expected " + t.getSimpleName() + ": [" + v + "]");
                    continue;
                }
                if (updateMap) map.put(key, v);
            } else if (t.isArray() && v != null) {
                Class vt = v.getClass();
                if (vt.isArray() && vt.getComponentType() == Object.class) {
                    if (t.getComponentType() == String.class) {
                        Object[] va = (Object[])v;
                        String[] sa = new String[va.length];
                        for (int i = 0; i < va.length; i++) sa[i] = (String)va[i];
                        v = sa;
                    }
                }
            }
            try {
                f.set(dto, v);
            } catch (Exception x) {
                error(key, "Invalid value, expected " + f.getType().getSimpleName() + ": [" + v + "]");
            }
        }
        return this;
    }

    private String[] parseStringArray(String s) throws IOException {
        if (s.length() == 0) return new String[0];
        if (s.charAt(0) == '[') return jsonService.fromJson(new ByteArrayInputStream(s.getBytes("UTF8")), String[].class);
        return s.split("[\\s]*,[\\s]*");
    }

    private void error(String field, String message) {
        if (errors == null) errors = new LinkedHashMap<String, String>();
        errors.put(field, message);
    }

    /**
     * If there are any errors throw a {@link DataBindingException}.
     */
    public void check() throws DataBindingException {
        if (errors != null) throw new DataBindingException(errors);
    }

}
