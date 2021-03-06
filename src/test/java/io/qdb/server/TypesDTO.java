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

package io.qdb.server;

import java.util.Date;

/**
 * For {@link DataBinderSpec}.
 */
public class TypesDTO {

    public boolean boolValue;
    public boolean boolWrapperValue;
    public int intValue;
    public Integer intWrapperValue;
    public long longValue;
    public Long longWrapperValue;
    public String stringValue;
    public Date dateValue;
    public String[] stringArrayValue;

}
