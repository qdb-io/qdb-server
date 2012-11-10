package qdb.io.server;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FileMetaDataStoreTest {

    private static File baseDir = new File("build/test-data");

    @BeforeClass
    public static void beforeClass() throws IOException {
        create(baseDir);
    }

    private static File create(File dir) throws IOException {
        if (dir.exists()) delete(dir);
        if (!dir.mkdirs()) {
            throw new IOException("Unable to create [" + dir + "]");
        }
        return dir;
    }

    private static File create(String subdir) throws IOException {
        return create(new File(baseDir, subdir));
    }

    @SuppressWarnings("ConstantConditions")
    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File file : f.listFiles()) delete(file);
        }
        if (!f.delete()) throw new IOException("Unable to delete [" + f + "]");
    }

    public static class Node {

        private String data;

        public Node(String data) {
            this.data = data;
        }

        public Node() {
        }

        public String getData() {
            return data;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Node && data.equals(((Node) obj).data);
        }

        @Override
        public String toString() {
            return data;
        }
    }

    @Test
    public void testSetAndGet() throws IOException {
        FileMetaDataStore mds = new FileMetaDataStore(create("setandget"));
        Node n = new Node("item");

        mds.set("foo", n);
        assertEquals(n, mds.get("foo", Node.class));

        mds.set("foo/bar/baz", n);
        assertEquals(n, mds.get("foo/bar/baz", Node.class));
    }

    @Test
    public void testList() throws IOException {
        FileMetaDataStore mds = new FileMetaDataStore(create("list"));
        Node n = new Node("item");

        mds.set("foo", n);
        mds.set("foo/bar/baz", n);
        mds.set("piggy/oinks", n);

        // there are 3 files (foo.json, foo and piggy) but foo.json and foo are the same node
        String[] list = mds.list("");
        assertEquals(2, list.length);
        assertEquals("foo", list[0]);
        assertEquals("piggy", list[1]);
    }

    @Test
    public void testDelete() throws IOException {
        File dir = create("delete");
        FileMetaDataStore mds = new FileMetaDataStore(dir);
        Node n = new Node("item");
        mds.set("foo/bar/baz", n);
        mds.delete("foo");
        assertEquals(0, dir.list().length);
    }

}
