package io.qdb.server;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.qdb.server.controller.Router;
import io.qdb.server.model.Repository;
import io.qdb.server.zoo.ZooRepository;
import org.simpleframework.http.core.Container;

import java.lang.reflect.Proxy;

/**
 * Standard server configuration.
 */
public class StdModule extends AbstractModule {

    @Override
    protected void configure() {
        Config cfg = ConfigFactory.load();
        ConfigUtil.bindProperties(binder(), cfg);

//        Repository repo = (Repository)Proxy.newProxyInstance(Repository.class.getClassLoader(),
//                new Class[]{Repository.class}, new CallTimeHandler());

        bind(Config.class).toInstance(cfg);
        bind(Container.class).to(Router.class);
        bind(Repository.class).to(ZooRepository.class);
        bind(EventBus.class).toInstance(new EventBus());
    }
}
