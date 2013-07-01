package io.qdb.server;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.typesafe.config.Config;
import me.moocar.logbackgelf.GelfAppender;
import org.slf4j.LoggerFactory;

/**
 * Configures logging if using logback.
 */
public class LogbackLogging {

    public void init(Config cfg) {
        org.slf4j.Logger slf4jRoot = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        if (!(slf4jRoot instanceof Logger)) return;

        String logLevel = System.getProperty("qdbLogLevel", cfg.getString("logLevel"));
        Logger root = (Logger)slf4jRoot;
        root.setLevel(Level.toLevel(logLevel));

        String graylog2 = cfg.getString("graylog2");
        if (graylog2 != null && graylog2.length() > 0) {
            int i = graylog2.indexOf(":");
            int port;
            if (i > 0) {
                port = Integer.parseInt(graylog2.substring(i + 1));
                graylog2 = graylog2.substring(0, i);
            } else {
                port = 12201;
            }

            GelfAppender<ILoggingEvent> ga = new GelfAppender<ILoggingEvent>();
            ga.setName("graylog2");
            ga.setFacility("qdb");
            ga.setGraylog2ServerHost(graylog2);
            ga.setGraylog2ServerPort(port);
            ga.setGraylog2ServerVersion(cfg.getString("graylog2ServerVersion"));
            ga.setUseLoggerName(true);
            ga.setUseThreadName(true);
            root.addAppender(ga);
            ga.start();
        }
    }

}
