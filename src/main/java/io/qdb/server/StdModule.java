package io.qdb.server;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.qdb.server.controller.Router;
import io.qdb.server.model.Repository;
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
        bind(Repository.class).to(SimpleRepository.class);
    }
}
