package io.qdb.server.databind;

/**
 * Parses numbers accepting suffixes of k, m and g for kb, mb and gb respectively.
 */
public class IntegerParser {

    public static final IntegerParser INSTANCE = new IntegerParser();

    public long parseLong(String s) throws NumberFormatException {
        int f = parseFactor(s);
        if (f > 1) s = s.substring(0, s.length() - 1);
        return Long.parseLong(s) * f;
    }

    public int parseInt(String s) throws NumberFormatException {
        int f = parseFactor(s);
        if (f > 1) s = s.substring(0, s.length() - 1);
        return Integer.parseInt(s) * f;
    }

    private int parseFactor(String s) {
        int n = s.length();
        int f = 1;
        if (n > 0) {
            char c = s.charAt(n - 1);
            switch (c) {
                case 'K':
                case 'k':
                    f = 1024;
                    break;
                case 'M':
                case 'm':
                    f = 1024 * 1024;
                    break;
                case 'G':
                case 'g':
                    f = 1024 * 1024 * 1024;
                    break;
            }
        }
        return f;
    }
}
