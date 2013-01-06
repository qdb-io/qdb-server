package io.qdb.server.repo;

import com.google.inject.Inject;
import com.google.inject.Injector;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Selects a strategy for finding the servers in our cluster using the serverRegistry property.
 */
@Singleton
public class ServerRegistryProvider implements Provider<ServerRegistry> {

    private final ServerRegistry serverRegistry;

    @SuppressWarnings("unchecked")
    @Inject
    public ServerRegistryProvider(Injector injector, @Named("serverRegistry") String serverRegistry) {
        if ("fixed".equals(serverRegistry)) {
            this.serverRegistry = injector.getInstance(FixedServerRegistry.class);
        } else {
            try {
                Class cls = Class.forName(serverRegistry, true, getClass().getClassLoader());
                this.serverRegistry = (ServerRegistry)injector.getInstance(cls);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("serverRegistry class [" + serverRegistry + "] not found");
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("serverRegistry [" + serverRegistry + "] is not an instance of " +
                        ServerRegistry.class.getName());
            }
        }
    }

    @Override
    public ServerRegistry get() {
        return serverRegistry;
    }
}
