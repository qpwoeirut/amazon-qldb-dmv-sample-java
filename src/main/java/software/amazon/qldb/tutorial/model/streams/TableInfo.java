/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package software.amazon.qldb.tutorial.model.streams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Represents the table information that goes inside the {@link
 * RevisionDetailsRecord}. It allows the users to deserialize the {@link
 * Revision#data} appropriate to the underlying table.
 */
public final class TableInfo {
    private String tableId;
    private String tableName;

    @JsonCreator
    public TableInfo(@JsonProperty("tableId") String tableId, @JsonProperty("tableName") String tableName) {
        this.tableId = tableId;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableId='" + tableId + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableInfo tableInfo = (TableInfo) o;

        if (!Objects.equals(tableId, tableInfo.tableId)) {
            return false;
        }
        return Objects.equals(tableName, tableInfo.tableName);
    }

    @Override
    public int hashCode() {
        int result = tableId != null ? tableId.hashCode() : 0;
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        return result;
    }
}