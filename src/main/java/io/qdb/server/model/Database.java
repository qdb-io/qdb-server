package io.qdb.server.model;

/**
 * Forms a namespace for queues.
 */
public class Database extends ModelObject {

    private String owner;

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
}
