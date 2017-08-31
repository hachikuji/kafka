/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol.types;

public enum Fields implements Fieldable {
    ERROR_CODE(Type.INT16, "error_code", "Response error code"),
    TOPIC(Type.STRING, "topic", "Name of the topic"),
    PARTITION(Type.INT32, "partition", "Topic partition identifier");

    public final Type type;
    public final String fieldName;
    public final String fieldDoc;

    Fields(Type type, String fieldName, String fieldDoc) {
        this.type = type;
        this.fieldName = fieldName;
        this.fieldDoc = fieldDoc;
    }

    public Field toField() {
        return new Field(fieldName, type, fieldDoc);
    }

}
