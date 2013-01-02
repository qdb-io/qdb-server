package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import io.qdb.server.model.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keeps model objects in a map.
 */
class ModelStore<T extends ModelObject> {

    private final ConcurrentMap<String, T> map;
    private final EventBus eventBus;
    private ModelEvent.Factory<T> eventFactory;

    ModelStore(Map<String, T> map, EventBus eventBus) {
        this.map = map == null ? new ConcurrentHashMap<String, T>() : new ConcurrentHashMap<String, T>(map);
        this.eventBus = eventBus;
    }

    public void setEventFactory(ModelEvent.Factory<T> eventFactory) {
        this.eventFactory = eventFactory;
    }

    @SuppressWarnings("unchecked")
    public T find(String id) {
        T o = map.get(id);
        return o == null ? null : (T)o.clone();
    }

    public T create(T o) {
        if (map.putIfAbsent(o.getId(), o) != null) throw new DuplicateIdException("Duplicate id " + tos(o));
        if (eventFactory != null) eventBus.post(eventFactory.createEvent(ModelEvent.Type.ADDED, o));
        return o;
    }

    public T update(T o) {
        ModelObject existing = map.get(o.getId());
        if (existing == null) throw new OptLockException(tos(o) + " not found");
        if (existing.getVersion() != o.getVersion()) {
            throw new OptLockException(tos(o) + " has incorrect version " + o.getVersion() +
                    " (expected " + existing.getVersion() + ")");
        }
        o.incVersion();
        map.put(o.getId(), o);
        return o;
    }

    @SuppressWarnings("unchecked")
    public List<T> find(int offset, int limit) throws IOException {
        if (limit < 0) limit = Integer.MAX_VALUE - offset;
        List<T> list = new ArrayList<T>(map.values());
        Collections.sort(list);
        int n = list.size();
        if (offset == 0 && limit >= n) return list;
        return offset >= n ? Collections.EMPTY_LIST : list.subList(offset, Math.min(limit, n));
    }

    public int size() throws IOException {
        return map.size();
    }

    public Map<String, T> copy() {
        return new HashMap<String, T>(map);
    }

    private static String tos(ModelObject o) {
        return o == null ? "null" : o.getClass().getSimpleName() + ":" + o.getId();
    }

}
