/*
 * Copyright (C) Gustav Karlsson
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.kagkarlsson.scheduler.jdbc;

import static com.github.kagkarlsson.scheduler.jdbc.Queries.selectForUpdate;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MssqlJdbcCustomization extends DefaultJdbcCustomization {

  public MssqlJdbcCustomization() {
    super(true);
  }

  public MssqlJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
    warnIfNotPersistingInUTC(persistTimestampInUTC, getClass());
  }

  @Override
  public void setTaskData(PreparedStatement p, int index, byte[] value) throws SQLException {
    p.setBytes(index, value);
  }

  @Override
  public void requestWork(JdbcWorkParams request) {
    var task = request.instance().getTaskInstance();
    var jdbcRunner = request.jdbcRunner();
    // HOLDLOCK protects an absent key as well, avoiding concurrent MERGE inserts.
    var query =
        "merge into "
            + request.tableName()
            + " with (HOLDLOCK, UPDLOCK) target using (values (?, ?)) as source(task_name, task_instance)"
            + " on (target.task_name = source.task_name and target.task_instance = source.task_instance)"
            + " when matched then update set rerun = ?"
            + " when not matched then insert (task_name, task_instance, task_data, execution_time, picked, rerun, version"
            + (request.orderByPriority() ? ", priority" : "")
            + ") values (?, ?, ?, ?, ?, ?, ?"
            + (request.orderByPriority() ? ", ?" : "")
            + ");";
    jdbcRunner.execute(
        query,
        ps -> {
          int index = 1;
          ps.setString(index++, task.getTaskName()); // source.task_name
          ps.setString(index++, task.getId()); // source.task_instance
          ps.setBoolean(index++, true); // matched: rerun
          ps.setString(index++, task.getTaskName()); // insert: task_name
          ps.setString(index++, task.getId()); // insert: task_instance
          setTaskData(ps, index++, request.taskData());
          setInstant(ps, index++, request.instance().getExecutionTime());
          ps.setBoolean(index++, false); // insert: picked
          ps.setBoolean(index++, false); // insert: rerun
          ps.setLong(index++, 1L); // insert: version
          if (request.orderByPriority()) {
            ps.setInt(index++, task.getPriority());
          }
        });
  }

  @Override
  public String getName() {
    return "MSSQL";
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    // Currently supported, but not recommended because of deadlock issues
    return true;
  }

  @Override
  public String createSelectDueQuery(
      String tableName, int limit, String andCondition, boolean orderByPriority) {
    return "SELECT "
        + " * FROM "
        + tableName
        // try reading past locked rows to see if that helps on deadlock-warnings
        + " WITH (READPAST) WHERE picked = ? AND execution_time <= ? "
        + andCondition
        + Queries.ansiSqlOrderPart(orderByPriority)
        + getQueryLimitPart(limit);
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition, boolean orderByPriority) {
    return selectForUpdate(
        tableName,
        Queries.ansiSqlOrderPart(orderByPriority),
        Queries.ansiSqlLimitPart(limit),
        requiredAndCondition,
        null,
        " WITH (READPAST,ROWLOCK) ",
        null);
  }
}
