package io.qdb.server.zoo;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import io.qdb.server.JsonService;
import io.qdb.server.model.Database;
import io.qdb.server.model.ModelException;
import io.qdb.server.model.ModelObject;
import io.qdb.server.model.User;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Caches users, databases etc.
 */
class ZooModelCache<T extends ModelObject> implements Closeable {

    private final Class<T> modelCls;
    private final JsonService jsonService;
    private final String path;
    private final CuratorFramework client;
    private final PathChildrenCache cache;

    ZooModelCache(Class<T> modelCls, JsonService jsonService, CuratorFramework client, String path) throws Exception {
        this.modelCls = modelCls;
        this.jsonService = jsonService;
        this.path = path;
        this.client = client;
        cache = new PathChildrenCache(client, path, true);
        cache.start();
    }

    @Override
    public void close() throws IOException {
        cache.close();
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
            throw new ModelException("[" + path + "] already exists");
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
}
