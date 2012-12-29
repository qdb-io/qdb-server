package io.qdb.server.controller;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.Timeline;
import io.qdb.server.JsonService;
import io.qdb.server.ServerId;
import io.qdb.server.model.*;
import io.qdb.server.model.Queue;
import io.qdb.server.queue.QueueManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.regex.Pattern;

@Singleton
public class TimelineController extends CrudController {

    private final QueueManager queueManager;
    private final String serverId;

    static class TimelineEntryDTO {

        public long messageId;
        public long timestamp;
        public int bytes;
        public long millis;
        public int count;

        TimelineEntryDTO(Timeline t, int i) {
            messageId = t.getMessageId(i);
            timestamp = t.getTimestamp(i);
            bytes = t.getBytes(i);
            millis = t.getMillis(i);
            count = t.getCount(i);
        }
    }

    @Inject
    public TimelineController(JsonService jsonService, QueueManager queueManager, ServerId serverId) {
        super(jsonService);
        this.queueManager = queueManager;
        this.serverId = serverId.get();
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        Queue q = call.getQueue();
        if (!q.isMaster(serverId) && !q.isSlave(serverId)) {
            // todo set Location header and send a 302 or proxy the master
            call.setCode(500, "Get timeline from non-master non-slave not implemented");
            return;
        }
        MessageBuffer mb = queueManager.getBuffer(q);
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }

        Timeline timeline = mb.getTimeline();
        TimelineEntryDTO[] ans = new TimelineEntryDTO[timeline == null ? 0 : timeline.size()];
        for (int i = 0; i < ans.length; i++) ans[i] = new TimelineEntryDTO(timeline, i);
        call.setJson(ans);
    }
}
