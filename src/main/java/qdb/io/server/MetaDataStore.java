package qdb.io.server;

import java.io.IOException;

/**
 * Keeps dynamic server configuration information. Modelled after a filesystem except that nodes can contain data
 * and other nodes i.e. 'files' are also 'directories'. Configuration objects are serialized to/from JSON using
 * Jackson.
 */
public interface MetaDataStore {

    /**
     * List all the child nodes of path sorted in alpha order.
     */
    String[] list(String path) throws IOException;

    /**
     * Get the contents of the node at path or null if it does not exist.
     */
    <T> T get(String path, Class<T> valueType) throws IOException;

    /**
     * Set the contents of the node at path.
     */
    void set(String path, Object data) throws IOException;

    /**
     * Delete the node at path. This will also delete all of its children.
     */
    void delete(String path) throws IOException;
}
