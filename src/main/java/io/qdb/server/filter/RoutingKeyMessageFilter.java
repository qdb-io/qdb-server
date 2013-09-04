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
            if ("*".equals(routingKey)) {   // special case where * does not match an empty word
                regex = "[^\\.]+";
            } else {
                String[] a = routingKey.split("\\.");
                StringBuilder b = new StringBuilder();
                int prev = -1;
                for (String term : a) {
                    int current = term.length() == 0 ? 0 : term.charAt(0);
                    if (current == '*') {
                        if (prev != -1) {
                            if (prev == '#') b.append("(\\.|^)");
                            else b.append("\\.");
                        }
                        b.append("[^\\.]*");
                    } else if (current == '#') {
                        if (prev == '#') continue; // eliminate consecutive '#'
                        if (prev != -1) b.append("(\\..*)?");
                        else b.append(".*");
                    } else {
                        if (prev != -1) {
                            if (prev == '#') b.append("(\\.|^)");
                            else b.append("\\.");
                        }
                        b.append(Pattern.quote(term));
                    }
                    prev = current;
                }
                regex = b.toString();
            }
        }
        try {
            pattern = Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid routingKey [" + routingKey + "]: " + e.getMessage());
        }
    }

    @Override
    public Result accept(long id, long timestamp, String routingKey, byte[] payload) {
        return pattern.matcher(routingKey).matches() ? Result.ACCEPT : Result.REJECT;
    }
}
