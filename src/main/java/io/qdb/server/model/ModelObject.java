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

/**
 * Base class for objects in our model. Supports equals (class and id must match) and hashcode (on id).
 * Serializable to/from JSON with Genson.
 */
public abstract class ModelObject implements Cloneable, Comparable<ModelObject> {

    private String id;
    private int version;

    protected ModelObject() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void incVersion() {
        ++version;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass().isAssignableFrom(getClass()) && id.equals(((ModelObject)o).id);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + id;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ignore) {
            return null; // not possible since we are Cloneable
        }
    }

    @Override
    public int compareTo(ModelObject o) {
        return id.compareTo(o.id);
    }
}
