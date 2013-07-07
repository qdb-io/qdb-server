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

package io.qdb.server.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A queue.
 */
public class Queue extends ModelObject {

    private String database;
    private long maxSize;
    private int maxPayloadSize;
    private String contentType;
    private Map<String, String> outputs;

    public Queue() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public int getMaxPayloadSize() {
        return maxPayloadSize;
    }

    public void setMaxPayloadSize(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Map<String, String> getOutputs() {
        return outputs;
    }

    public void setOutputs(Map<String, String> outputs) {
        this.outputs = outputs;
    }

    public String getOidForOutput(String output) {
        return outputs == null ? null : outputs.get(output);
    }

    public String getOutputForOid(String oid) {
        if (outputs != null) {
            for (Map.Entry<String, String> e : outputs.entrySet()) {
                if (oid.equals(e.getValue())) return e.getKey();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return super.toString() + ":database=" + database;
    }

    public Queue deepCopy() {
        Queue q = (Queue)clone();
        if (outputs != null) q.outputs = new HashMap<String, String>(outputs);
        return q;
    }
}
