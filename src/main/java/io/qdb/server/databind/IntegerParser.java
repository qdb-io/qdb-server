/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.databind;

/**
 * Parses numbers accepting suffixes of k, kb, m, mb, g and gb for kb, mb and gb respectively. If there is a suffix
 * the number can be a double.
 */
public class IntegerParser {

    public static final IntegerParser INSTANCE = new IntegerParser();

    public long parseLong(String s) throws NumberFormatException {
        int n = s.length();
        int f = parseFactor(s, n);
        if (f > 1) {
            char c = s.charAt(--n);
            if (c == 'b' || c == 'B') --n;
            s = s.substring(0, n).trim();
            if (s.indexOf('.') >= 0) return (long)(Double.parseDouble(s) * f);
        }
        return Long.parseLong(s) * f;
    }

    public int parseInt(String s) throws NumberFormatException {
        int n = s.length();
        int f = parseFactor(s, n);
        if (f > 1) {
            char c = s.charAt(--n);
            if (c == 'b' || c == 'B') --n;
            s = s.substring(0, n).trim();
            if (s.indexOf('.') >= 0) return (int)(Double.parseDouble(s) * f);
        }
        return Integer.parseInt(s) * f;
    }

    private int parseFactor(String s, int n) {
        int f = 1;
        if (n > 0) {
            char c = s.charAt(n - 1);
            if ( n > 1 && c == 'b' || c == 'B') c = s.charAt(n - 2);
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
