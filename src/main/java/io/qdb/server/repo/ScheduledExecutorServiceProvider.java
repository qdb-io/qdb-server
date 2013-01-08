package io.qdb.server.repo;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Creates the thread pool used by cluster components.
 */
@Singleton
@Named("clusterPool")
public class ScheduledExecutorServiceProvider implements Provider<ScheduledExecutorService>, Closeable,
        Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ScheduledExecutorServiceProvider.class);

    private final ScheduledExecutorService pool;

    @Inject
    public ScheduledExecutorServiceProvider() {
        pool = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("cluster-pool-%d").setUncaughtExceptionHandler(this).build());
    }

    @Override
    public ScheduledExecutorService get() {
        return pool;
    }

    @Override
    public void close() throws IOException {
        pool.shutdownNow();
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error(e.toString(), e);
    }
}
