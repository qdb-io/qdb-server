package io.qdb.server.model;

import java.util.Iterator;
import java.util.Map;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String database;
    private long maxSize;
    private int maxPayloadSize;
    private String contentType;
    private Map<String, String> outputs;

    public Queue() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public int getMaxPayloadSize() {
        return maxPayloadSize;
    }

    public void setMaxPayloadSize(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Map<String, String> getOutputs() {
        return outputs;
    }

    public void setOutputs(Map<String, String> outputs) {
        this.outputs = outputs;
    }

    public String getOidForOutput(String output) {
        return outputs == null ? null : outputs.get(output);
    }

    public String getOutputForOid(String oid) {
        if (outputs != null) {
            for (Map.Entry<String, String> e : outputs.entrySet()) {
                if (oid.equals(e.getValue())) return e.getKey();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return super.toString() + ":database=" + database;
    }
}
