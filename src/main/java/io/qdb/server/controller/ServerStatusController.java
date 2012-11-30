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

    private final Renderer renderer;

    @Inject
    public ServerStatusController(Renderer renderer) {
        this.renderer = renderer;
    }

    public void index(Call call) throws IOException {
        Map map = new HashMap();
        map.put("wibble", "wobble");
        renderer.renderJson(call, map);
    }

}
