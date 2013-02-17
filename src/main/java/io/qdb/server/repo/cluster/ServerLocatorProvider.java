package io.qdb.server.repo.cluster;

import com.google.inject.Inject;
import com.google.inject.Injector;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Selects a strategy for finding the servers in our cluster using the serverRegistry property.
 */
@Singleton
public class ServerLocatorProvider implements Provider<ServerLocator> {

    private final ServerLocator serverLocator;

    @SuppressWarnings("unchecked")
    @Inject
    public ServerLocatorProvider(Injector injector, @Named("serverLocator") String serverLocator) {
        if ("fixed".equals(serverLocator)) {
            this.serverLocator = injector.getInstance(FixedServerLocator.class);
        } else {
            try {
                Class cls = Class.forName(serverLocator, true, getClass().getClassLoader());
                this.serverLocator = (FixedServerLocator)injector.getInstance(cls);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("serverLocator class [" + serverLocator + "] not found");
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("serverLocator [" + serverLocator + "] is not an instance of " +
                        ServerLocator.class.getName());
            }
        }
    }

    @Override
    public ServerLocator get() {
        return serverLocator;
    }
}
