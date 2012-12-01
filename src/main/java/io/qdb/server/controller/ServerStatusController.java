package io.qdb.server.controller;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Status of the server.
 */
@Singleton
public class ServerStatusController {

    @Inject
    public ServerStatusController() {
    }

    public void show(Call call) throws IOException {
        if (!call.isGet()) {
            call.setCode(400);
            return;
        }

        Map map = new HashMap();
        map.put("wibble", "wobble");
        call.setJson(map);
    }

}
