package io.qdb.server.controller;

import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.Timeline;
import io.qdb.server.queue.QueueManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

@Singleton
public class TimelineController extends CrudController {

    private final QueueManager queueManager;

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
    public TimelineController(JsonService jsonService, QueueManager queueManager) {
        super(jsonService);
        this.queueManager = queueManager;
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        MessageBuffer mb = queueManager.getBuffer(call.getQueue());
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }
        setTimeline(call, mb.getTimeline());
    }

    private void setTimeline(Call call, Timeline timeline) throws IOException {
        TimelineEntryDTO[] ans = new TimelineEntryDTO[timeline == null ? 0 : timeline.size()];
        for (int i = 0; i < ans.length; i++) ans[i] = new TimelineEntryDTO(timeline, i);
        call.setJson(ans);
    }

    @Override
    protected void show(Call call, String id) throws IOException {
        long messageId;
        try {
            messageId = Long.parseLong(id);
        } catch (NumberFormatException e) {
            call.setCode(400, "Invalid message id [" + id + "]");
            return;
        }

        MessageBuffer mb = queueManager.getBuffer(call.getQueue());
        if (mb == null || !mb.isOpen()) {
            // probably we are busy starting up and haven't synced this queue yet or are shutting down
            call.setCode(503, "Queue is not available, please try again later");
            return;
        }

        setTimeline(call, mb.getTimeline(messageId));
    }
}
