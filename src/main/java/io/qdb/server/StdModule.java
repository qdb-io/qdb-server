package io.qdb.server;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.qdb.server.controller.Router;
import io.qdb.server.security.SimpleUserRepository;
import io.qdb.server.security.UserRepository;
import org.simpleframework.http.core.Container;

/**
 * Standard server configuration.
 */
public class StdModule extends AbstractModule {

    @Override
    protected void configure() {
        Config cfg = ConfigFactory.load();
        ConfigUtil.bindProperties(binder(), cfg);

        bind(Config.class).toInstance(cfg);
        bind(Container.class).to(Router.class);
        bind(UserRepository.class).to(SimpleUserRepository.class);
    }
}
