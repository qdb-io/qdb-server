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

import humanize.Humanize;

import java.io.File;
import java.io.IOException;

/**
 * Static utility methods for working with durations.
 */
public class DurationParser {

    private static final int DAY_MS = 24 * 60 * 60 * 1000;

    /**
     * Convert a ms value into a human readable duration.
     */
    public static String formatHumanMs(long ms) {
        StringBuilder b = new StringBuilder();
        int days = (int)(ms / DAY_MS);
        if (days > 0) {
            b.append(days).append(days == 1 ? " day " : " days ");
            ms %= DAY_MS;
        }
        int secs = (int)(ms / 1000);
        if (secs < 1 && days == 0) {
            ms %= 1000;
            b.append(ms).append(" ms");
        } else {
            b.append(Humanize.duration(secs));
        }
        return b.toString();
    }

    /**
     * Parse a duration in '2 days 2:02:32' format into seconds. Throws IllegalArgumentException if invalid.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public static int parse(String s) throws IllegalArgumentException {
        try {
            int days = 0, hours = 0, mins = 0, secs = 0;
            int i = s.indexOf('d');
            if (i < 0) i = s.indexOf('D');
            if (i > 0) {
                days = Integer.parseInt(s.substring(0, i).trim());
                int n = s.length();
                for (++i; i < n && !Character.isDigit(s.charAt(i)); i++);
                s = i < n ? s.substring(i) : "";
            }
            if (s.length() > 0) {
                i = s.lastIndexOf(':');
                secs = Integer.parseInt(s.substring(i + 1));
                if (i > 0) {
                    int j = s.lastIndexOf(':', i - 1);
                    mins = Integer.parseInt(s.substring(j + 1, i));
                    if (j > 0) hours = Integer.parseInt(s.substring(0, j));
                }
            }
            return days * 24 * 60 * 60 + hours * 60 * 60 + mins * 60 + secs;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid duration [" + s + "]");
        }
    }

}
