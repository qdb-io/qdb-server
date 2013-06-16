package io.qdb.server.databind;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Date time parser that accepts several formats. Thread safe.
 */
public class DateTimeParser {

    private final SimpleDateFormat full = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
    private final SimpleDateFormat noTz = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private final SimpleDateFormat noSecs = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
    private final SimpleDateFormat dateOnly = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat secs = new SimpleDateFormat("HH:mm:ss");
    private final SimpleDateFormat mins = new SimpleDateFormat("HH:mm");

    public static final DateTimeParser INSTANCE = new DateTimeParser();

    public Date parse(String s) throws ParseException {
        Date ans;
        switch (s.length()) {
            case 5:
                synchronized (mins) { ans = mins.parse(s); }
                timeToToday(ans);
                break;
            case 8:
                synchronized (secs) { ans = secs.parse(s); }
                timeToToday(ans);
                break;
            case 10:
                synchronized (dateOnly) { ans = dateOnly.parse(s); }
                break;
            case 16:
                synchronized (noSecs) { ans = noSecs.parse(s); }
                break;
            case 19:
                synchronized (noTz) { ans = noTz.parse(s); }
                break;
            default:
                synchronized (full) { ans = full.parse(s); }
        }
        return ans;
    }

    @SuppressWarnings("deprecation")
    private void timeToToday(Date d) {
        GregorianCalendar gc = new GregorianCalendar();
        d.setYear(gc.get(Calendar.YEAR) - 1900);
        d.setMonth(gc.get(Calendar.MONTH));
        d.setDate(gc.get(Calendar.DAY_OF_MONTH));
    }
}
