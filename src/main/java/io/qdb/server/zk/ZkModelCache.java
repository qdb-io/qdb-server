package io.qdb.server.zk;

import com.google.common.eventbus.EventBus;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import io.qdb.server.JsonService;
import io.qdb.server.model.DuplicateIdException;
import io.qdb.server.model.ModelEvent;
import io.qdb.server.model.ModelObject;
import io.qdb.server.model.OptLockException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Caches users, databases etc.. Broadcasts events when new objects are added/updated.
 */
class ZkModelCache<T extends ModelObject> implements Closeable, PathChildrenCacheListener {

    private static final Logger log = LoggerFactory.getLogger(ZkModelCache.class);

    private final Class<T> modelCls;
    private final JsonService jsonService;
    private final String path;
    private final CuratorFramework client;
    private final PathChildrenCache cache;
    private final EventBus eventBus;
    private final ModelEvent.Factory<T> eventFactory;

    ZkModelCache(Class<T> modelCls, JsonService jsonService, CuratorFramework client, String path, EventBus eventBus,
                 ModelEvent.Factory<T> eventFactory) throws Exception {
        this.modelCls = modelCls;
        this.jsonService = jsonService;
        this.path = path;
        this.client = client;
        this.eventBus = eventBus;
        this.eventFactory = eventFactory;
        cache = new PathChildrenCache(client, path, true);
        if (eventFactory != null) cache.getListenable().addListener(this);
        cache.start(true);
    }

    @Override
    public void close() throws IOException {
        cache.close();
    }

    public String getPath() {
        return path;
    }

    /**
     * Find the object with id or null if none.
     */
    public T find(String id) throws IOException {
        try {
            String path = this.path + "/" + id;
            ChildData cd = cache.getCurrentData(path);
            if (cd != null) return toModelObject(cd);
            T ans = jsonService.fromJson(client.getData().forPath(path), modelCls);
            cache.clearAndRefresh(); // cache is out of sync
            return ans;
        } catch (KeeperException.NoNodeException ignore) {
            return null;
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
    }

    public List<T> list(int offset, int limit) throws IOException {
        if (limit < 0) limit = Integer.MAX_VALUE - offset;
        List<T> ans = new ArrayList<T>();
        List<ChildData> data = cache.getCurrentData();
        for (int i = offset, n = Math.min(offset + limit, data.size()); i < n; i++) ans.add(toModelObject(data.get(i)));
        return ans;
    }

    public int size() {
        return cache.getCurrentData().size();
    }

    @SuppressWarnings("unchecked")
    public T create(T modelObject) throws IOException {
        assert modelObject.getId() != null;
        String path = this.path + "/" + modelObject.getId();
        try {
            T o = (T)modelObject.clone();
            o.setId(null);
            client.create().forPath(path, jsonService.toJson(o));
            return modelObject;
        } catch (KeeperException.NodeExistsException e) {
            throw new DuplicateIdException("[" + path + "] already exists");
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public T update(T modelObject) throws IOException {
        assert modelObject.getId() != null;
        String path = this.path + "/" + modelObject.getId();
        try {
            T o = (T)modelObject.clone();
            o.setId(null);
            Stat stat = client.setData().withVersion(modelObject.getVersion()).forPath(path, jsonService.toJson(o));
            modelObject.setVersion(stat.getVersion());
            return modelObject;
        } catch (KeeperException.BadVersionException e) {
            throw new OptLockException(e.getMessage());
        } catch (Exception e) {
            throw new IOException(e.toString(), e);
        }
    }

    private T toModelObject(ChildData cd) throws IOException {
        T ans = jsonService.fromJson(cd.getData(), modelCls);
        ans.setId(getLastPart(cd.getPath()));
        ans.setVersion(cd.getStat().getVersion());
        return ans;
    }

    private String getLastPart(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        log.debug("childEvent " + event);
        ModelEvent.Type type;
        switch (event.getType()) {
            case CHILD_ADDED:       type = ModelEvent.Type.ADDED;       break;
            case CHILD_UPDATED:     type = ModelEvent.Type.UPDATED;     break;
            default:                return;
        }
        eventBus.post(eventFactory.createEvent(type, toModelObject(event.getData())));
    }
}
