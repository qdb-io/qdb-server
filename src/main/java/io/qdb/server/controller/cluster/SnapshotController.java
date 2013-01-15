package io.qdb.server.controller.cluster;

import io.qdb.server.controller.Call;
import io.qdb.server.controller.CrudController;
import io.qdb.server.controller.JsonService;
import io.qdb.server.repo.StandaloneRepository;
import org.simpleframework.http.Response;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * New servers joining a cluster use this endpoint to download a snapshot of the meta data from the master
 * server.
 */
@Singleton
public class SnapshotController extends CrudController {

    private final StandaloneRepository repo;

    @Inject
    public SnapshotController(JsonService jsonService, StandaloneRepository repo) {
        super(jsonService);
        this.repo = repo;
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        if (!"latest".equals(id)) {
            call.setCode(404);
            return;
        }
        StandaloneRepository.Snapshot snapshot = repo.createSnapshot();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream out = new GZIPOutputStream(bos);
        jsonService.toJsonNoIndenting(out, snapshot);
        out.close();
        byte[] data = bos.toByteArray();

        Response response = call.getResponse();
        response.set("Content-Encoding", "gzip");
        response.set("Content-Type", "application/json; charset=utf-8");
        response.setContentLength(data.length);
        response.getOutputStream().write(data);
    }
}
