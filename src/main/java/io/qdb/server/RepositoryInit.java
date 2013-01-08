package io.qdb.server;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import io.qdb.server.model.Repository;
import io.qdb.server.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;

/**
 * Makes sure that we always have an admin user in the repository.
 */
@Singleton
public class RepositoryInit {

    private static final Logger log = LoggerFactory.getLogger(RepositoryInit.class);

    private final Repository repo;
    private final OurServer ourServer;
    private final String initialAdminPassword;

    @Inject
    public RepositoryInit(Repository repo, EventBus eventBus, OurServer ourServer,
                @Named("initialAdminPassword") String initialAdminPassword) {
        this.repo = repo;
        this.ourServer = ourServer;
        this.initialAdminPassword = initialAdminPassword;
        eventBus.register(this);
        handleRepoStatusChange(repo.getStatus());
    }

    @Subscribe
    public void handleRepoStatusChange(Repository.Status status) {
        if (status.isUp() && (status.clusterName == null || ourServer.equals(status.master))) {
            ensureAdminUser();
        }
    }

    private void ensureAdminUser() {
        try {
            if (repo.findUser("admin") == null) {
                User admin = new User();
                admin.setId("admin");
                admin.setPassword(initialAdminPassword);
                admin.setAdmin(true);
                repo.createUser(admin);
                log.info("Created initial admin user");
            }
        } catch (Exception e) {
            log.error(e.toString(), e);
        }
    }
}
