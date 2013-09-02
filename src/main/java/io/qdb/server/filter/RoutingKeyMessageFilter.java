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
            if ("*".equals(routingKey)) {   // special case where * does not match an empty word
                regex = "[^\\.]+";
            } else {
                String[] a = routingKey.split("\\.");
                StringBuilder b = new StringBuilder();
                int prev = -1;
                for (String term : a) {
                    int current = term.length() == 0 ? 0 : term.charAt(0);
                    if (current == '*') {
                        if (prev != -1) b.append("\\.");
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
                        b.append(term);
//                    for (int j = 0, n = term.length(); j < n; j++) {
//                        b.append("\\u");
//                        int c = term.charAt(j);
//                        b.append(HEX[(c >> 12) & 0xf]);
//                        b.append(HEX[(c >> 8) & 0xf]);
//                        b.append(HEX[(c >> 4) & 0xf]);
//                        b.append(HEX[c & 0xf]);
//                    }
                    }
                    prev = current;
                }
                regex = b.toString();
            }
        }
        System.out.println("routingKey " + routingKey + " -> /" + regex + "/");
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
