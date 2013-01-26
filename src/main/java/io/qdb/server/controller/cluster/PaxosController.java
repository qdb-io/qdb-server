package io.qdb.server.controller.cluster;

import com.google.common.eventbus.EventBus;
import io.qdb.server.controller.Call;
import io.qdb.server.controller.Controller;
import io.qdb.server.repo.JsonSerializer;
import io.qdb.server.repo.cluster.PaxosMasterStrategy;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

/**
 * Receives Paxos messages for master election and posts them on the event bus.
 */
@Singleton
public class PaxosController implements Controller {

    private final EventBus eventBus;
    private final JsonSerializer jsonConverter;

    @Inject
    public PaxosController(EventBus eventBus, JsonSerializer jsonConverter) {
        this.eventBus = eventBus;
        this.jsonConverter = jsonConverter;
    }

    @Override
    public void handle(Call call) throws IOException {
        if (call.isPost()) {
            String serverId = call.getRequest().getValue("Referer");
            if (serverId == null) {
                call.setText(400, "Missing 'Referer' HTTP Header");
                return;
            }

            PaxosMasterStrategy.Msg msg;
            InputStream ins = Channels.newInputStream(call.getRequest().getByteChannel());
            try {
                msg = jsonConverter.readValue(ins, PaxosMasterStrategy.Msg.class);
            } finally {
                ins.close();
            }
            eventBus.post(new PaxosMasterStrategy.Delivery(msg, serverId));
            call.setCode(201);
        } else {
            call.setCode(400);
        }
    }
}
