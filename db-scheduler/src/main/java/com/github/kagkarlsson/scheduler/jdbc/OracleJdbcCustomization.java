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

import com.github.kagkarlsson.jdbc.SQLRuntimeException;
import java.sql.SQLException;
import java.sql.SQLTransientException;

public class OracleJdbcCustomization extends DefaultJdbcCustomization {

  public OracleJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
  }

  @Override
  public void requestWork(JdbcWorkParams request) {
    var task = request.instance().getTaskInstance();
    var jdbcRunner = request.jdbcRunner();
    var query =
        "merge into "
            + request.tableName()
            + " target using (select ? task_name, ? task_instance from dual) source"
            + " on (target.task_name = source.task_name and target.task_instance = source.task_instance)"
            + " when matched then update set rerun = ?"
            + " when not matched then insert (task_name, task_instance, task_data, execution_time, picked, rerun, version"
            + (request.orderByPriority() ? ", priority" : "")
            + ") values (?, ?, ?, ?, ?, ?, ?"
            + (request.orderByPriority() ? ", ?" : "")
            + ")";
    try {
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
    } catch (SQLRuntimeException e) {
      if (!isDuplicateKey(e)) {
        throw e;
      }
      // Oracle rolls back the failed statement, preserving the surrounding transaction. The winner
      // may already have completed; use one current UPDATE, never a read/retry loop.
      int updated =
          jdbcRunner.execute(
              "update "
                  + request.tableName()
                  + " set rerun = ?"
                  + " where task_name = ? and task_instance = ?",
              ps -> {
                ps.setBoolean(1, true);
                ps.setString(2, task.getTaskName());
                ps.setString(3, task.getId());
              });
      if (updated == 0) {
        throw new SQLRuntimeException(
            new SQLTransientException(
                "Concurrent execution completed during requestWork; retry the transaction.", e));
      }
    }
  }

  private boolean isDuplicateKey(Throwable exception) {
    for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
      if (cause instanceof SQLException sql && sql.getErrorCode() == 1) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getName() {
    return "Oracle";
  }

  @Override
  public boolean supportsGenericLockAndFetch() {
    // FIXLATER: fix syntax and enable
    return false;
  }

  @Override
  public String createGenericSelectForUpdateQuery(
      String tableName, int limit, String requiredAndCondition, boolean orderByPriority) {
    return selectForUpdate(
        tableName,
        Queries.ansiSqlOrderPart(orderByPriority),
        Queries.ansiSqlLimitPart(limit),
        requiredAndCondition,
        " FOR UPDATE SKIP LOCKED ",
        null,
        null);
  }
}
