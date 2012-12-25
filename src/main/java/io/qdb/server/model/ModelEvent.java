package io.qdb.server.model;

/**
 * Fired by the repository when objects are created, updated etc.
 */
public class ModelEvent<T extends ModelObject> {

    public enum Type { ADDED, UPDATED }

    private final Type type;
    private final T object;

    public interface Factory<T extends ModelObject> {
        ModelEvent<T> createEvent(Type type, T object);
    }

    public ModelEvent(Type type, T object) {
        this.type = type;
        this.object = object;
    }

    public Type getType() {
        return type;
    }

    public T getObject() {
        return object;
    }

    @Override
    public String toString() {
        return type + " " + object;
    }
}
