package io.qdb.server.databind;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds keys and values from a map to objects. Accumulates errors.
 */
public class DataBinder {

    private boolean ignoreInvalidFields;
    private Map<String, String> errors;

    public DataBinder() {
    }

    public DataBinder ignoreInvalidFields(boolean on) {
        this.ignoreInvalidFields = on;
        return this;
    }

    /**
     * Bind the values of map to matching public field names from dto. Support basic data types and does conversions
     * from String to these types. If dto implements {@link HasAnySetter} the keys from the map that do not match
     * field names are passed to {@link HasAnySetter#set(String, Object)}.
     */
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
            Class t;
            if (v instanceof String && (t = f.getType()) != String.class) {
                String s = (String)v;
                try {
                    if (t == Integer.TYPE || t == Integer.class) v = Integer.parseInt(s);
                    else if (t == Long.TYPE || t == Long.class) v = Long.parseLong(s);
                    else if (t == Boolean.TYPE || t == Boolean.class) v = "true".equals(v);
                } catch (Exception x) {
                    error(key, "Invalid value, expected " + t.getSimpleName() + ": [" + v + "]");
                    continue;
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
