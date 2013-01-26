package io.qdb.server.model;

import java.util.Arrays;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String database;
    private String master;
    private String[] slaves;
    private long maxSize;
    private int maxPayloadSize;
    private String contentType;

    public Queue() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String[] getSlaves() {
        return slaves;
    }

    public void setSlaves(String[] slaves) {
        this.slaves = slaves;
    }

    public boolean isMaster(String serverId) {
        return serverId.equals(master);
    }

    public boolean isSlave(String serverId) {
        if (slaves != null) {
            for (String slave : slaves) {
                if (serverId.equals(slave)) return true;
            }
        }
        return false;
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

    @Override
    public String toString() {
        return super.toString() + ":database=" + database + ",master=" + master +
                ",slaves=" + (slaves == null ? "null" : Arrays.asList(slaves).toString());
    }
}
