package io.qdb.server.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import com.google.common.io.PatternFilenameFilter;
import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.buffer.PersistentMessageBuffer;
import io.qdb.server.controller.JsonService;
import io.qdb.server.Util;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.*;
import java.util.*;

/**
 * Keeps our meta data in maps in memory with periodic snapshots written to disk. Uses a MessageBuffer as a tx log
 * for replay from the last snapshot after a crash.
 */
@Singleton
public class StandaloneRepository implements Repository, Closeable {

    private static final Logger log = LoggerFactory.getLogger(StandaloneRepository.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final File dir;
    private final int snapshotCount;
    private final int snapshotIntervalSecs;
    private final Timer snapshotTimer;

    private ModelStore<User> users;
    private ModelStore<Database> databases;
    private ModelStore<Queue> queues;

    private MessageBuffer txLog;
    private Date upSince;
    private long mostRecentSnapshotId;
    private boolean busySavingSnapshot;
    private boolean snapshotScheduled;

    private static class Snapshot {

        public List<User> users;
        public List<Database> databases;
        public List<Queue> queues;

        public Snapshot() { }

        public Snapshot(StandaloneRepository repo) throws IOException {
            users = repo.users.values();
            databases = repo.databases.values();
            queues = repo.queues.values();
        }
    }

    @Inject
    public StandaloneRepository(EventBus eventBus,
                @Named("dataDir") String dataDir,
                @Named("txLogSizeM") int txLogSizeM,
                @Named("snapshotCount") int snapshotCount,
                @Named("snapshotIntervalSecs") int snapshotIntervalSecs,
                @Named("snapshotPretty") boolean snapshotPretty) throws IOException {
        this.snapshotCount = snapshotCount;
        this.snapshotIntervalSecs = snapshotIntervalSecs;

        dir = Util.ensureDirectory(new File(dataDir));

        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        if (snapshotPretty) mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        txLog = new PersistentMessageBuffer(Util.ensureDirectory(new File(dir, "txlog")));
        txLog.setMaxSize(txLogSizeM * 1000000);
        txLog.setMaxPayloadSize(100000);

        File[] files = getSnapshotFiles();
        Snapshot snapshot = null;
        for (int i = files.length - 1; i >= 0; i--) {
            File f = files[i];
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
            try {
                snapshot = mapper.readValue(in, Snapshot.class);
            } catch (Exception e) {
                log.error("Error loading " + f + ", ignoring: " + e);
                continue;
            } finally {
                try {
                    in.close();
                } catch (IOException ignore) {
                }
            }

            String name = f.getName();
            int j = name.indexOf('-');
            int k = name.lastIndexOf('.');
            mostRecentSnapshotId = Long.parseLong(name.substring(j + 1, k), 16);
            if (log.isDebugEnabled()) log.debug("Loaded " + f);
            break;
        }

        if (mostRecentSnapshotId < txLog.getOldestMessageId()) {
            throw new IOException("Most recent snapshot " + mostRecentSnapshotId +  " is older than oldest record " +
                    "in txlog " + txLog.getOldestMessageId());
        }

        users = new ModelStore<User>(snapshot == null ? null : snapshot.users, eventBus);
        databases = new ModelStore<Database>(snapshot == null ? null : snapshot.databases, eventBus);
        queues = new ModelStore<Queue>(snapshot == null ? null : snapshot.queues, eventBus);

        int count = 0;
        for (MessageCursor c = txLog.cursor(mostRecentSnapshotId); c.next(); count++) {
            RepoTx tx = mapper.readValue(c.getPayload(), RepoTx.class);
            try {
                apply(tx);
            } catch (ModelException e) {
                if (log.isDebugEnabled()) log.debug("Got " + e + " replaying " + tx);
            }
        }
        if (log.isDebugEnabled()) log.debug("Replayed " + count + " transaction(s)");

        queues.setEventFactory(Queue.Event.FACTORY); // set this now to avoid firing events when playing the tx log

        snapshotTimer = new Timer("repo-snapshot", true);

        upSince = new Date();
        eventBus.post(getStatus());
    }

    private File[] getSnapshotFiles() {
        File[] files = dir.listFiles(new PatternFilenameFilter("snapshot-[0-9a-f]+.json"));
        Arrays.sort(files);
        return files;
    }

    @Override
    public void close() throws IOException {
        snapshotTimer.cancel();
        txLog.close();
    }

    /**
     * Save a snapshot. This is a NOP if we are already busy saving a snapshot or if no new transactions have been
     * applied since the most recent snapshot was saved.
     */
    void saveSnapshot() throws IOException {
        Snapshot snapshot;
        long id;
        try {
            synchronized (this) {
                if (busySavingSnapshot) return;
                busySavingSnapshot = true;
                txLog.sync();
                id = txLog.getNextMessageId();
                if (id == mostRecentSnapshotId) return; // nothing to do
                snapshot = new Snapshot(this);
            }
            File f = new File(dir, "snapshot-" + String.format("%016x", id) + ".json");
            if (log.isDebugEnabled()) log.debug("Creating " + f);
            boolean ok = false;
            FileOutputStream out = new FileOutputStream(f);
            try {
                mapper.writeValue(out, snapshot);
                out.close();
                synchronized (this) {
                    mostRecentSnapshotId = id;
                }
                ok = true;
            } finally {
                if (!ok) {
                    try {
                        out.close();
                    } catch (IOException ignore) {
                    }
                    if (!f.delete()) {
                        log.error("Unable to delete bad snapshot: " + f);
                    }
                }
            }

            deleteOldSnapshots();

        } finally {
            synchronized (this) {
                busySavingSnapshot = false;
            }
        }
    }

    private void deleteOldSnapshots() {
        File[] a = getSnapshotFiles();
        for (int i = 0; i < (a.length - snapshotCount); i++) {
            if (a[i].delete()) {
                if (log.isDebugEnabled()) log.debug("Deleted " + a[i]);
            } else {
                log.error("Unable to delete " + a[i]);
            }
        }
    }

    /**
     * Append tx to our tx log and apply it to our in memory model.
     */
    private void exec(RepoTx tx) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(tx);
        long timestamp = System.currentTimeMillis();
        boolean snapshotNow;
        synchronized (this) {
            txLog.append(timestamp, null, payload);
            apply(tx);
            long bytes = txLog.getNextMessageId() - mostRecentSnapshotId;
            snapshotNow = bytes > txLog.getMaxSize() / 2; // half our log space is gone so do a snapshot now
        }
        if (snapshotNow) {
            saveSnapshot();
        } else {
            scheduleSnapshot();
        }
    }

    private synchronized void scheduleSnapshot() {
        if (!snapshotScheduled) {
            snapshotTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        synchronized (StandaloneRepository.this) {
                            snapshotScheduled = false;
                        }
                        saveSnapshot();
                    } catch (Throwable e) {
                        log.error("Error saving snapshot: " + e, e);
                    }
                }
            }, snapshotIntervalSecs * 1000L);
        }
    }

    /**
     * Apply tx to our in memory model.
     */
    @SuppressWarnings("unchecked")
    private void apply(RepoTx tx) {
        ModelStore store = getStore(tx.object);
        switch (tx.op) {
            case CREATE:    store.create(tx.object);    break;
            case UPDATE:    store.update(tx.object);    break;
            default:        throw new IllegalStateException("Unknown operation " + tx.op);
        }
    }

    @SuppressWarnings("unchecked")
    private ModelStore getStore(ModelObject o) {
        if (o instanceof Database) return databases;
        if (o instanceof Queue) return queues;
        if (o instanceof User) return users;
        throw new IllegalStateException("Unknown object type " + o);
    }

    @Override
    public Status getStatus() {
        Status s = new Status();
        s.upSince = upSince;
        return s;
    }

    @Override
    public void createQdbNode(QdbNode node) throws IOException {
    }

    @Override
    public List<QdbNode> findQdbNodes() throws IOException {
        return null;
    }

    @Override
    public User findUser(String id) throws IOException {
        return users.find(id);
    }

    @Override
    public User createUser(User user) throws IOException {
        exec(new RepoTx(RepoTx.Operation.CREATE, user));
        return user;
    }

    @Override
    public User updateUser(User user) throws IOException {
        exec(new RepoTx(RepoTx.Operation.UPDATE, user));
        return user;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<User> findUsers(int offset, int limit) throws IOException {
        return users.find(offset, limit);
    }

    @Override
    public int countUsers() throws IOException {
        return users.size();
    }

    @Override
    public Database findDatabase(String id) throws IOException {
        return databases.find(id);
    }

    @Override
    public Database createDatabase(Database db) throws IOException {
        exec(new RepoTx(RepoTx.Operation.CREATE, db));
        return db;
    }

    @Override
    public Database updateDatabase(Database db) throws IOException {
        exec(new RepoTx(RepoTx.Operation.UPDATE, db));
        return db;
    }

    @Override
    public List<Database> findDatabasesVisibleTo(User user, int offset, int limit) throws IOException {
        if (user.isAdmin()) {
            return databases.find(offset, limit);
        } else {
            ArrayList<Database> ans = new ArrayList<Database>();
            String[] databases = user.getDatabases();
            if (databases != null) {
                for (int i = offset, n = Math.min(offset + limit, databases.length); i < n; i++) {
                    Database db = findDatabase(databases[i]);
                    if (db != null) ans.add(db);
                }
            }
            return ans;
        }
    }

    @Override
    public int countDatabasesVisibleTo(User user) throws IOException {
        if (user.isAdmin()) {
            return databases.size();
        } else {
            String[] databases = user.getDatabases();
            return databases == null ? 0 : databases.length;
        }
    }

    @Override
    public Queue findQueue(String id) throws IOException {
        return queues.find(id);
    }

    @Override
    public Queue createQueue(Queue queue) throws IOException {
        exec(new RepoTx(RepoTx.Operation.CREATE, queue));
        return queue;
    }

    @Override
    public Queue updateQueue(Queue queue) throws IOException {
        exec(new RepoTx(RepoTx.Operation.UPDATE, queue));
        return queue;
    }

    @Override
    public List<Queue> findQueues(int offset, int limit) throws IOException {
        return queues.find(offset, limit);
    }

    @Override
    public int countQueues() throws IOException {
        return queues.size();
    }

}
