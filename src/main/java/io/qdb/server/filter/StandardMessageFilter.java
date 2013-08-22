package io.qdb.server.filter;

import io.qdb.server.model.Queue;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Combines {@link RoutingKeyMessageFilter} and {@link GrepMessageFilter}. This is the filter used when both
 * routingKey and grep parameters are supplied without a filter parameter.
 */
public class StandardMessageFilter implements MessageFilter {

    public String routingKey;
    public String grep;

    private final RoutingKeyMessageFilter routingKeyMessageFilter = new RoutingKeyMessageFilter();
    private final GrepMessageFilter grepMessageFilter = new GrepMessageFilter();

    @Override
    public void init(Queue q) throws IllegalArgumentException {
        routingKeyMessageFilter.routingKey = routingKey;
        routingKeyMessageFilter.init(q);
        grepMessageFilter.grep = grep;
        grepMessageFilter.init(q);
    }

    @Override
    public Result accept(long timestamp, String routingKey, byte[] payload) {
        if (payload == null) {
            if (routingKeyMessageFilter.accept(timestamp, routingKey, payload) == Result.REJECT) return Result.REJECT;
        }
        return grepMessageFilter.accept(timestamp, routingKey, payload);
    }
}
