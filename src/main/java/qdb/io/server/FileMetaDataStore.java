package qdb.io.server;

import com.typesafe.config.Config;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import javax.inject.Inject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Stores meta data on the filesystem in json files.
 */
public class FileMetaDataStore implements MetaDataStore {

    private final File dir;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final String[] EMPTY = new String[0];

    @SuppressWarnings("deprecation")
    @Inject
    public FileMetaDataStore(Config cfg) {
        this(new File(cfg.getString("metadata.dir")));
    }

    public FileMetaDataStore(File dir) {
        this.dir = dir;
        if (dir.exists() && !dir.isDirectory()) {
            throw new BadConfigException("Not a directory metadata.dir: [" + dir + "]");
        }
        if (!dir.exists() && !dir.mkdirs()) {
            throw new BadConfigException("Unable to create metadata.dir: [" + dir + "]");
        }
        if (!dir.canWrite()) {
            throw new BadConfigException("Unable to write to metadata.dir: [" + dir + "]");
        }

        // todo should search tree for .old files with no corresponding .json and rename them

        mapper.configure(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, false);
        mapper.configure(SerializationConfig.Feature.WRITE_EMPTY_JSON_ARRAYS, false);
        mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public String[] list(String path) throws IOException {
        File f = new File(dir, path);
        if (!f.exists()) return EMPTY;
        String[] list = f.list();
        if (list == null) throw new IOException("Unable to list files in [" + f + "]");
        Set<String> set = new HashSet<String>();
        for (String name : list) {
            if (name.endsWith(".json")) {
                set.add(name.substring(0, name.length() - 5));
            } else {
                if (new File(f, name).isDirectory()) set.add(name);
            }
        }
        set.toArray(list = new String[set.size()]);
        Arrays.sort(list);
        return list;
    }

    @Override
    public <T> T get(String path, Class<T> valueType) throws IOException {
        File f = toFile(path);
        if (!f.isFile()) return null;
        return mapper.readValue(f, valueType);
    }

    private File toFile(String path) {
        return new File(dir, path + ".json");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void set(String path, Object data) throws IOException {
        String json = mapper.writeValueAsString(data);

        File f = toFile(path);
        File p = f.getParentFile();
        if (!p.exists() && !p.mkdirs()) {
            throw new IOException("Unable to create directory [" + p + "]");
        }
        File nf = new File(dir, path + ".new" + System.identityHashCode(this));
        File of = new File(dir, path + ".old" + System.identityHashCode(this));

        FileWriter fw = new FileWriter(nf);
        try {
            fw.write(json);
        } finally {
            fw.close();
        }

        of.delete();
        if (f.exists()) {
            if (!f.renameTo(of)) throw new IOException("Unable to rename [" + f + "] to [" + of + "]");
        }
        if (!nf.renameTo(f)) {
            of.renameTo(f);
            throw new IOException("Unable to rename [" + nf + "] to [" + f + "]");
        }
        if (of.exists() && !of.delete()) throw new IOException("Unable to delete [" + of + "]");
    }

    @Override
    public void delete(String path) throws IOException {
        File f = toFile(path);
        if (f.exists()) delete(f);
        f = new File(dir, path);
        if (f.exists()) delete(f);
    }

    private void delete(File f) throws IOException {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            if (files == null) {
                throw new IOException("Unable to list files in [" + f + "]");
            }
            for (File file : files) delete(file);
        }
        if (!f.delete()) throw new IOException("Unable to delete [" + f + "]");
    }
}
