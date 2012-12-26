package io.qdb.server.model;

import java.util.Map;

/**
 * Forms a namespace for queues.
 */
public class Database extends ModelObject {

    private String owner;
    private Map<String, String> queues;

    public Database() {
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isVisibleTo(User user) {
        return user.getId().equals(owner) || user.isAdmin() || user.canReadDatabase(getId());
    }

    public Map<String, String> getQueues() {
        return queues;
    }

    public void setQueues(Map<String, String> queues) {
        this.queues = queues;
    }

    public String getQid(String id) {
        return queues == null ? null : queues.get(id);
    }
}
