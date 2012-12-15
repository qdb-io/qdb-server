package io.qdb.server.model;

/**
 * Forms a namespace for queues.
 */
public class Database extends ModelObject {

    private String ownerUserId;

    public Database() {
    }

    public String getOwnerUserId() {
        return ownerUserId;
    }

    public void setOwnerUserId(String ownerUserId) {
        this.ownerUserId = ownerUserId;
    }

    public boolean isVisibleTo(User user) {
        return user.getId().equals(ownerUserId) || user.isAdmin() || user.canReadDatabase(getId());
    }
}
