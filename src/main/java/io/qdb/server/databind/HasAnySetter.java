package io.qdb.server.databind;

import java.util.Map;

/**
 * DTOs that can contain any key and value implement this.
 */
public interface HasAnySetter {

    public void set(String key, Object value);

    public Map<String, Object> getParams();

}
