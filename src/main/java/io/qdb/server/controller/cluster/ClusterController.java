package io.qdb.server.controller.cluster;

import io.qdb.server.controller.Call;
import io.qdb.server.controller.CrudController;
import io.qdb.server.controller.JsonService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.xml.bind.DatatypeConverter;
import java.io.IOException;

/**
 * These operations are used by the servers in a QDB cluster to exchange meta data, elect leaders etc..
 */
@Singleton
public class ClusterController extends CrudController {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class);

    private final boolean clustered;
    private final String clusterName;
    private final String clusterPassword;

    @Inject
    public ClusterController(JsonService jsonService,
            @Named("clustered") boolean clustered,
            @Named("clusterName") String clusterName,
            @Named("clusterPassword") String clusterPassword) {
        super(jsonService);
        this.clustered = clustered;
        this.clusterName = clusterName;
        this.clusterPassword = clusterPassword;
    }

    @Override
    public void handle(Call call) throws IOException {
        if (clustered) {
            if (isAuthenticated(call)) {
                super.handle(call);
            } else {
                call.setCode(401);
            }
        } else {
            call.setCode(404, "Clustering is disabled");
        }
    }

    private boolean isAuthenticated(Call call) throws IOException {
        String s = call.getRequest().getValue("Authorization");
        if (s == null) return false;

        if (s.startsWith("Basic ")) {
            try {
                s = new String(DatatypeConverter.parseBase64Binary(s.substring(6)), "UTF8");
                int i = s.indexOf(':');
                if (i > 0) {
                    String username = s.substring(0, i);
                    if (!username.equals(clusterName)) {
                        log.warn("Incorrect cluster name [" + clusterName + "] instead of [" + clusterName +
                                "] received from " + call.getRequest().getClientAddress());
                        return false;
                    }
                    String password = s.substring(i + 1);
                    if (!password.equals(clusterPassword)) {
                        log.warn("Incorrect cluster password received from " + call.getRequest().getClientAddress());
                        return false;
                    }
                    return true;

                }
            } catch (IllegalArgumentException ignore) {
            }
        }
        return false;
    }
}
