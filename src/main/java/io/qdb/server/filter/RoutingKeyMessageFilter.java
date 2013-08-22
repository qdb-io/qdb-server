package io.qdb.server.filter;

import io.qdb.server.model.Queue;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Accepts messages with routingKey matching a regex or a RabbitMQ style matching expression.
 */
public class RoutingKeyMessageFilter implements MessageFilter {

    public String routingKey;

    private Pattern pattern;

    private static final char[] HEX = new char[]{'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

    @Override
    public void init(Queue q) throws IllegalArgumentException {
        if (routingKey == null || routingKey.length() == 0) throw new IllegalArgumentException("routingKey is required");
        String regex;
        if (routingKey.charAt(0) == '/') {
            int n = routingKey.length();
            if (n > 1 && routingKey.charAt(n - 1) == '/') --n;
            regex = routingKey.substring(1, n);
        } else {
            // convert RabbitMQ style matching expression to regex
            String[] a = routingKey.split("\\.");
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < a.length; i++) {
                if (i > 0) b.append("\\.");
                String w = a[i];
                if ("#".equals(w)) {            // match one or more words
                    b.append(".*");
                } else if ("*".equals(w)) {     // match any one word
                    b.append("[^\\.]*");
                } else {                        // match exactly
                    for (int j = 0, n = w.length(); j < n; j++) {
                        b.append("\\u");
                        int c = w.charAt(j);
                        b.append(HEX[(c >> 12) & 0xf]);
                        b.append(HEX[(c >> 8) & 0xf]);
                        b.append(HEX[(c >> 4) & 0xf]);
                        b.append(HEX[c & 0xf]);
                    }
                }
            }
            regex = b.toString();
        }
        try {
            pattern = Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid routingKey [" + routingKey + "]: " + e.getMessage());
        }
    }

    @Override
    public Result accept(long timestamp, String routingKey, byte[] payload) {
        return pattern.matcher(routingKey).matches() ? Result.ACCEPT : Result.REJECT;
    }
}
