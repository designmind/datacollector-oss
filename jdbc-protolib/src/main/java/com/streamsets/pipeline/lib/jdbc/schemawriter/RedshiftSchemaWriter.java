/*
 * Copyright 2021 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.jdbc.schemawriter;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcType;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;
import com.zaxxer.hikari.HikariDataSource;

public class RedshiftSchemaWriter extends JdbcAbstractSchemaWriter {
    private static final Map<JdbcType, String> jdbcTypeToName = ImmutableMap.<JdbcType, String>builder()
        .put(JdbcType.BIGINT, "bigint")
        .put(JdbcType.FLOAT, "real")
        .put(JdbcType.DOUBLE, "double precision")
        .put(JdbcType.DECIMAL, "decimal")
        .put(JdbcType.SHORT, "smallint")
        .put(JdbcType.INTEGER, "integer")
        .put(JdbcType.CHAR, "char")
        .put(JdbcType.VARCHAR, "varchar(65535)")
        .put(JdbcType.DATE, "date")
        .put(JdbcType.TIME, "time")
        .put(JdbcType.TIMESTAMP, "timestamp")
        .put(JdbcType.TIMESTAMP_WITH_TIME_ZONE, "timestamptz")
        .put(JdbcType.BINARY, "varchar(65535)")
        .put(JdbcType.BOOLEAN, "boolean")
        .build();
    private static final int MAX_PRECISION = 16383;
    private static final int MAX_SCALE = 131072;
    private static final String DEFAULT_SCHEMA = "public";

    public RedshiftSchemaWriter(HikariDataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected int getMaxScale() {
        return MAX_SCALE;
    }

    @Override
    protected int getMaxPrecision() {
        return MAX_PRECISION;
    }

    @Override
    public String getColumnTypeName(JdbcType jdbcType) {
        return jdbcTypeToName.get(jdbcType);
    }

    @Override
    public String getDefaultSchema() {
        return DEFAULT_SCHEMA;
    }

    // Redshift wants an ALTER TABLE command per column
    @Override
    protected String makeAlterTableSqlString(String schema, String tableName,
            LinkedHashMap<String, JdbcTypeInfo> columnDiff) {
        String tableSchema = (schema == null) ? getDefaultSchema() : schema;
        StringBuilder sqlString = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, JdbcTypeInfo> entry : columnDiff.entrySet()) {
            if (first) {
                first = false;
            } else {
                sqlString.append("\n");
            }
            sqlString.append(ALTER_TABLE).append(" ");
            if (tableSchema != null) {
                sqlString.append(tableSchema);
                sqlString.append(".");
            }
            sqlString.append(tableName)
                .append(" ")
                .append("ADD COLUMN")
                .append(" ")
                .append('"')
                .append(entry.getKey())
                .append('"')
                .append(" ")
                .append(entry.getValue().toString())
                .append(";");
        }

        return sqlString.toString();
    }

    public static String getConnectionPrefix() {
        return "jdbc:redshift:";
    }
}