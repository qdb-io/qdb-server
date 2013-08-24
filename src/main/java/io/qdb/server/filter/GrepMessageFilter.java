package io.qdb.server.filter;

import io.qdb.server.model.Queue;

import java.io.UnsupportedEncodingException;
import java.nio.charset.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Accepts messages with payload as a String matching a regex. The contentType of the queue is used to find the
 * encoding of the payload with UTF8 being the default.
 */
public class GrepMessageFilter implements MessageFilter {

    public String grep;

    private Pattern pattern;
    private String encoding;

    @Override
    public void init(Queue q) throws IllegalArgumentException {
        try {
            pattern = Pattern.compile(grep, Pattern.MULTILINE);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid grep regex [" + grep + "]: " + e.getMessage());
        }
        String contentType = q == null ? "UTF8" : q.getContentType();

        encoding = "UTF8";  // todo get this from contentType
        try {
            Charset.forName(encoding);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid queue charset in contentType [" + contentType + "]");
        }
    }

    @Override
    public Result accept(long id, long timestamp, String routingKey, byte[] payload) {
        if (payload == null) return Result.CHECK_PAYLOAD;
        String s;
        try {
            s = new String(payload, encoding);
        } catch (UnsupportedEncodingException e) {  // this shouldn't happen
            return Result.REJECT;
        }
        return pattern.matcher(s).find() ? Result.ACCEPT : Result.REJECT;
    }
}
