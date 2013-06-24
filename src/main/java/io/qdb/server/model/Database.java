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

import java.util.Map;

/**
 * Forms a namespace for queues.
 */
public class Database extends ModelObject {

    private String owner;
    private Map<String, String> queues;

    public Database() {
    }

    public Database(String id) {
        setId(id);
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isVisibleTo(User user) {
        return user.getId().equals(owner) || user.isAdmin() || user.canReadDatabase(getId());
    }

    public Map<String, String> getQueues() {
        return queues;
    }

    public void setQueues(Map<String, String> queues) {
        this.queues = queues;
    }

    public String getQidForQueue(String queue) {
        return queues == null ? null : queues.get(queue);
    }

    public String getQueueForQid(String qid) {
        if (queues != null) {
            for (Map.Entry<String, String> e : queues.entrySet()) {
                if (qid.equals(e.getValue())) return e.getKey();
            }
        }
        return null;
    }
}
