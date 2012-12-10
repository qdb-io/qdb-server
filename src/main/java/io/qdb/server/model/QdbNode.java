package io.qdb.server.model;

/**
 * A qdb instance in our cluster.
 */
public class QdbNode extends ModelObject {

    private String url;
    private String tag1;
    private String tag2;

    public QdbNode() {
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTag1() {
        return tag1;
    }

    public void setTag1(String tag1) {
        this.tag1 = tag1;
    }

    public String getTag2() {
        return tag2;
    }

    public void setTag2(String tag2) {
        this.tag2 = tag2;
    }
}
