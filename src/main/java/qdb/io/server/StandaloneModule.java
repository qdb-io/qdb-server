package qdb.io.server;

import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.simpleframework.http.core.Container;

/**
 * Standalone (i.e. non-clustered) server configuration.
 */
public class StandaloneModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Config.class).toInstance(ConfigFactory.load());
        bind(Container.class).to(Router.class);
        bind(MetaDataStore.class).to(FileMetaDataStore.class);
    }
}
