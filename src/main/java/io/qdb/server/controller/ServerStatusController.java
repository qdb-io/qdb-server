package io.qdb.server.controller;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Date;

/**
 * Status of the server.
 */
@Singleton
public class ServerStatusController implements Controller {

    private final Date upSince = new Date();

    public static class StatusDTO {
        public Date upSince;
        public StatusDTO() { }
        public StatusDTO(Date upSince) { this.upSince = upSince; }
    }

    @Inject
    public ServerStatusController() {
    }

    @Override
    public void handle(Call call) throws IOException {
        if (!call.isGet()) {
            call.setCode(400);
            return;
        }
        call.setJson(new StatusDTO(upSince));
    }
}
