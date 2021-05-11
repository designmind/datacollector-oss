/*
 * Copyright 2017 StreamSets Inc.
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

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcType;
import com.streamsets.pipeline.lib.jdbc.typesupport.JdbcTypeInfo;
import com.zaxxer.hikari.HikariDataSource;

import java.util.LinkedHashMap;
import java.util.Map;

public class SQLServerSchemaWriter extends JdbcAbstractSchemaWriter {
  private static final Map<JdbcType, String> jdbcTypeToName = ImmutableMap.<JdbcType, String>builder()
      .put(JdbcType.BIGINT, "bigint")
      .put(JdbcType.FLOAT, "real")
      .put(JdbcType.DOUBLE, "float")
      .put(JdbcType.DECIMAL, "decimal")
      .put(JdbcType.SHORT, "smallint")
      .put(JdbcType.INTEGER, "int")
      .put(JdbcType.CHAR, "char")
      .put(JdbcType.VARCHAR, "varchar(max)")
      .put(JdbcType.DATE, "date")
      .put(JdbcType.TIME, "time")
      .put(JdbcType.TIMESTAMP, "datetime2")
      .put(JdbcType.TIMESTAMP_WITH_TIME_ZONE, "datetimeoffset")
      .put(JdbcType.BINARY, "binary")
      .put(JdbcType.BOOLEAN, "bit")
      .build();
  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 38;
  private static final String DEFAULT_SCHEMA = "public";

  public SQLServerSchemaWriter(HikariDataSource dataSource) {
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
  public String getDefaultSchema() { return DEFAULT_SCHEMA; }
  
  // SQL Server wants an ALTER TABLE add w/o column keyword
  @Override
  protected String makeAlterTableSqlString(
      String schema, String tableName, LinkedHashMap<String, JdbcTypeInfo> columnDiff
  ) {
    String tableSchema = (schema == null) ? getDefaultSchema() : schema;
    StringBuilder sqlString = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, JdbcTypeInfo> entry : columnDiff.entrySet()) {
      if (first) {
        first = false;
      } else {
        sqlString.append("\n");
      }
      sqlString
          .append(ALTER_TABLE)
          .append(" ");
      if (tableSchema != null) {
        sqlString.append(tableSchema);
        sqlString.append(".");
      }
      sqlString.append(tableName)
          .append(" ")
          .append("ADD")
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
      return "jdbc:sqlserver:";
  }
}
