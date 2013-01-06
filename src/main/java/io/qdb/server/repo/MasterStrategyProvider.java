package io.qdb.server.repo;

import com.google.inject.Inject;
import com.google.inject.Injector;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * Selects a strategy for selecting the master in the cluster using the masterStrategy property.
 */
@Singleton
public class MasterStrategyProvider implements Provider<MasterStrategy> {

    private final MasterStrategy masterStrategy;

    @SuppressWarnings("unchecked")
    @Inject
    public MasterStrategyProvider(Injector injector, @Named("masterStrategy") String masterStrategy) {
        if ("fixed".equals(masterStrategy)) {
            this.masterStrategy = injector.getInstance(FixedMasterStrategy.class);
        } else {
            try {
                Class cls = Class.forName(masterStrategy, true, getClass().getClassLoader());
                this.masterStrategy = (MasterStrategy)injector.getInstance(cls);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("masterStrategy class [" + masterStrategy + "] not found");
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("masterStrategy [" + masterStrategy + "] is not an instance of " +
                        MasterStrategy.class.getName());
            }
        }
    }

    @Override
    public MasterStrategy get() {
        return masterStrategy;
    }
}
