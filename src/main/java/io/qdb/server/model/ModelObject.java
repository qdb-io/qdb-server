package io.qdb.server.model;

/**
 * Base class for objects in our model. Supports equals (class and id must match) and hashcode (on id).
 * Serializable to/from JSON with Jackson.
 */
public abstract class ModelObject implements Cloneable {

    private String id;

    protected ModelObject() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass() == getClass() && id.equals(((ModelObject)o).id);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + id;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ignore) {
            return null; // not possible since we are Cloneable
        }
    }
}
