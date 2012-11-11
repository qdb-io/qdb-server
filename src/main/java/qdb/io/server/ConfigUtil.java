package qdb.io.server;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import java.util.Map;

/**
 * Static utility methods for configuration.
 */
public class ConfigUtil {

    /**
     * Creates a constant binding to {@code @Named(key)} for each entry in {@code cfg} that has a URL (i.e. is
     * not a system property or environment variable).
     */
    public static void bindProperties(Binder binder, Config cfg) {
        binder = binder.skipSources(ConfigUtil.class);
        for (Map.Entry<String, ConfigValue> entry : cfg.entrySet()) {
            ConfigValue value = entry.getValue();
            if (value.origin().url() != null) {
                Named named = Names.named(entry.getKey());
                Object v = value.unwrapped();
                if (v instanceof String) binder.bind(Key.get(String.class, named)).toInstance((String)v);
                else if (v instanceof Integer) binder.bind(Key.get(Integer.class, named)).toInstance((Integer)v);
                else binder.bind(Key.get(Object.class, named)).toInstance(v);
            }
        }
    }

}
