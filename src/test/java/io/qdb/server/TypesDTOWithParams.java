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
