package io.qdb.server.repo;

import com.google.common.eventbus.EventBus;
import com.google.common.io.PatternFilenameFilter;
import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.buffer.PersistentMessageBuffer;
import io.qdb.server.util.Util;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;

/**
 * Keeps our meta data in maps in memory with periodic snapshots written to disk. Uses a MessageBuffer as a tx log
 * for replay from the last snapshot after a crash.
 */
@Singleton
public class StandaloneRepository extends RepositoryBase {

    private static final Logger log = LoggerFactory.getLogger(StandaloneRepository.class);

    private final JsonConverter jsonConverter;
    private final File dir;
    private final int snapshotCount;
    private final int snapshotIntervalSecs;
    private final Timer snapshotTimer;

    private String repositoryId;
    private final ModelStore<User> users;
    private final ModelStore<Database> databases;
    private final ModelStore<Queue> queues;

    private MessageBuffer txLog;
    private Date upSince;
    private long mostRecentSnapshotId;
    private boolean busySavingSnapshot;
    private boolean snapshotScheduled;

    @SuppressWarnings("UnusedDeclaration")
    public static class Snapshot {

        public Long txId;
        public String repositoryId;
        public List<User> users;
        public List<Database> databases;
        public List<Queue> queues;

        public Snapshot() { }

        public Snapshot(StandaloneRepository repo) throws IOException {
            repositoryId = repo.repositoryId;
            users = repo.users.values();
            databases = repo.databases.values();
            queues = repo.queues.values();
        }
    }

    @Inject
    public StandaloneRepository(EventBus eventBus, JsonConverter jsonConverter,
                @Named("dataDir") String dataDir,
                @Named("txLogSizeM") int txLogSizeM,
                @Named("snapshotCount") int snapshotCount,
                @Named("snapshotIntervalSecs") int snapshotIntervalSecs) throws IOException {
        this.jsonConverter = jsonConverter;
        this.snapshotCount = snapshotCount;
        this.snapshotIntervalSecs = snapshotIntervalSecs;

        dir = Util.ensureDirectory(new File(dataDir, "meta-data"));

        txLog = new PersistentMessageBuffer(Util.ensureDirectory(new File(dir, "txlog")));
        txLog.setMaxSize(txLogSizeM * 1000000);
        txLog.setMaxPayloadSize(100000);

        File[] files = getSnapshotFiles();
        Snapshot snapshot = null;
        for (int i = files.length - 1; i >= 0; i--) {
            File f = files[i];
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
            try {
                snapshot = this.jsonConverter.readValue(in, Snapshot.class);
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
            throw new IOException("Most recent snapshot " + Long.toHexString(mostRecentSnapshotId) +
                    " is older than oldest record in txlog " + Long.toHexString(txLog.getOldestMessageId()));
        }

        if (txLog.getNextMessageId() == 0 && mostRecentSnapshotId > 0) {
            // probably this a recovery after a cluster failure by copying snapshot files around and nuking tx logs
            // to get everyone in sync
            log.info("The txlog is empty but we have snapshot " + Long.toHexString(mostRecentSnapshotId) +
                    " so using that as next id");
            txLog.setFirstMessageId(mostRecentSnapshotId);
        }

        boolean noSnapshot = snapshot == null;
        repositoryId = noSnapshot ? generateRepositoryId() : snapshot.repositoryId;
        users = new ModelStore<User>(noSnapshot ? null : snapshot.users, eventBus);
        databases = new ModelStore<Database>(noSnapshot ? null : snapshot.databases, eventBus);
        queues = new ModelStore<Queue>(noSnapshot ? null : snapshot.queues, eventBus);

        int count = 0;
        for (MessageCursor c = txLog.cursor(mostRecentSnapshotId); c.next(); count++) {
            RepoTx tx = this.jsonConverter.readValue(c.getPayload(), RepoTx.class);
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
    }

    private File[] getSnapshotFiles() {
        File[] files = dir.listFiles(new PatternFilenameFilter("snapshot-[0-9a-f]+.json"));
        Arrays.sort(files);
        return files;
    }

    private String generateRepositoryId() {
        SecureRandom rnd = new SecureRandom();
        byte[] a = new byte[8];
        rnd.nextBytes(a);
        return new BigInteger(a).abs().toString(36);
    }

    @Override
    public void close() throws IOException {
        snapshotTimer.cancel();
        txLog.close();
    }

    /**
     * Create a snapshot of this repository including its txId.
     */
    public synchronized Snapshot createSnapshot() throws IOException {
        txLog.sync();
        Snapshot ans = new Snapshot(this);
        ans.txId = txLog.getNextMessageId();
        return ans;
    }

    /**
     * Initialize this repository from snapshot. The repository must be empty and the snapshot must include a
     * txId.
     */
    public synchronized void initFromSnapshot(Snapshot snapshot) throws IOException {
        if (!isEmpty()) throw new IllegalStateException("Repository is not empty");
        if (snapshot.txId == null) throw new IllegalArgumentException("Snapshot is missing txId");
        repositoryId = snapshot.repositoryId;
        txLog.setFirstMessageId(snapshot.txId);
        for (User user : snapshot.users) this.users.create(user);
        for (Database db : snapshot.databases) this.databases.create(db);
        for (Queue q : snapshot.queues) this.queues.create(q);
        saveSnapshot();
    }

    /**
     * Save a snapshot. This is a NOP if we are already busy saving a snapshot or if no new transactions have been
     * applied since the most recent snapshot was saved.
     */
    public void saveSnapshot() throws IOException {
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
                jsonConverter.writeValue(out, snapshot);
                out.flush();
                out.getChannel().force(true);
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
     * Append tx to our tx log and apply it to our in memory model. Throws ModelException for opt locking failures
     * and so on. Note that the tx is always appended to the log but the model is not actually updated if a
     * ModelException is thrown.
     */
    public long exec(RepoTx tx) throws IOException, ModelException {
        byte[] payload = jsonConverter.writeValueAsBytes(tx);
        long timestamp = System.currentTimeMillis();
        boolean snapshotNow = false;
        long txId;
        try {
            synchronized (this) {
                txId = txLog.append(timestamp, null, payload);
                try {
                    apply(tx);
                } finally {
                    long bytes = txLog.getNextMessageId() - mostRecentSnapshotId;
                    snapshotNow = bytes > txLog.getMaxSize() / 2; // half our log space is gone so do a snapshot now
                }
            }
            return txId;
        } finally {
            if (snapshotNow) saveSnapshot();
            else scheduleSnapshot();
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
            case DELETE:    store.delete(tx.object);    break;
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

    /**
     * Open a cursor to our tx log.
     */
    public MessageCursor openTxCursor(long id) throws IOException {
        return txLog.cursor(id);
    }

    /**
     * What will the next tx id be?
     */
    public long getNextTxId() throws IOException {
        return txLog.getNextMessageId();
    }


    /**
     * Does this repository hold no model objects? An empty repository can be initialized with a snapshot from
     * another repository.
     */
    public boolean isEmpty() throws IOException {
        return users.size() == 0 && databases.size() == 0 && queues.size() == 0;
    }

    @Override
    public String getRepositoryId() {
        return repositoryId;
    }

    @Override
    public Status getStatus() {
        Status s = new Status();
        s.upSince = upSince;
        return s;
    }

    @Override
    public User findUser(String id) throws IOException {
        return users.find(id);
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
    public List<Queue> findQueues(int offset, int limit) throws IOException {
        return queues.find(offset, limit);
    }

    @Override
    public int countQueues() throws IOException {
        return queues.size();
    }

    /**
     * Create a object that is used to wait for a specific tx to be applied to our model.
     */
    public TxMonitor createTxMonitor() throws IOException {
        return new TxMonitor();
    }

    public class TxMonitor implements Closeable {

        private final MessageCursor c;

        public TxMonitor() throws IOException {
            this.c = txLog.cursor(txLog.getNextMessageId());
        }

        /**
         * Wait up to timeoutMs for txId to show up and be applied to our model. Returns true if the tx was found or
         * false otherwise.
         */
        public boolean waitFor(long txId, int timeoutMs) throws IOException {
            while (timeoutMs > 0) {
                long start = System.currentTimeMillis();
                try {
                    if (c.next(timeoutMs)) {
                        if (c.getId() >= txId) {
                            // this sync block makes sure that the tx has actually been applied to our model before
                            // we return as the tx is appended to the log and applied in a sync block
                            synchronized (StandaloneRepository.this) {
                                if (log.isDebugEnabled()) log.debug("TxMonitor txId " + txId + " from master has been applied");
                                return true;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
                timeoutMs -= (int)(System.currentTimeMillis() - start);
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            c.close();
        }
    }

}
