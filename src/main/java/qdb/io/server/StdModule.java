package qdb.io.server;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.simpleframework.http.core.Container;

/**
 * Standard server configuration.
 */
public class StdModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Config.class).toInstance(ConfigFactory.load());
        bind(Container.class).to(Router.class);
    }
}
