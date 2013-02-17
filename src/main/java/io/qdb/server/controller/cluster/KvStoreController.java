package io.qdb.server.controller.cluster;

import io.qdb.server.controller.Call;
import io.qdb.server.controller.Controller;
import io.qdb.server.repo.cluster.KvStoreTransport;
import org.simpleframework.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.nio.channels.Channels;

/**
 * Receives kvstore related messages and delivers them to our {@link io.qdb.kvstore.cluster.Transport} implementation.
 */
@Singleton
public class KvStoreController implements Controller {

    private static final Logger log = LoggerFactory.getLogger(KvStoreController.class);

    private final KvStoreTransport transport;

    @Inject
    public KvStoreController(KvStoreTransport transport) {
        this.transport = transport;
    }

    @Override
    public void handle(Call call) throws IOException {
        if (call.isPost()) {
            String serverId = call.getRequest().getValue("Referer");
            if (serverId == null) {
                call.setText(400, "Missing 'Referer' HTTP Header");
                return;
            }

            InputStream ins = Channels.newInputStream(call.getRequest().getByteChannel());
            byte[] content;
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                transport.onMessageReceived(serverId, ins, bos);
                content = bos.toByteArray();
            } catch (Exception e) {
                log.error("Error processing msg from " + serverId + ": " + e, e);
                call.setText(500, "Internal server error: " + e);
                return;
            } finally {
                close(ins);
            }

            Response res = call.getResponse();
            res.set("Content-Type", "application/octet-stream");
            res.setContentLength(content.length);
            res.setCode(201);
            res.getOutputStream().write(content);
        } else {
            call.setCode(400);
        }
    }

    private void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                log.debug("Error closing " + c + ": " + e.toString());
            }
        }
    }
}
