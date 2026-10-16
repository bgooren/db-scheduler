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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

public class MySQLBaseJdbcCustomization extends DefaultJdbcCustomization {

  public MySQLBaseJdbcCustomization(boolean persistTimestampInUTC) {
    super(persistTimestampInUTC);
    warnIfNotPersistingInUTC(persistTimestampInUTC, getClass());
  }

  @Override
  public void requestWork(JdbcWorkParams request) {
    var task = request.instance().getTaskInstance();
    var jdbcRunner = request.jdbcRunner();
    var query =
        "insert into "
            + request.tableName()
            + " (task_name, task_instance, task_data, execution_time, picked, rerun, version"
            + (request.orderByPriority() ? ", priority" : "")
            + ") values (?, ?, ?, ?, ?, ?, ?"
            + (request.orderByPriority() ? ", ?" : "")
            + ")"
            + " on duplicate key update rerun = ?";
    jdbcRunner.execute(
        query,
        ps -> {
          int index = 1;
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
          ps.setBoolean(index, true); // conflict: rerun
        });
  }

  @Override
  public void setInstant(PreparedStatement p, int index, Instant value) throws SQLException {
    if (value == null) {
      p.setTimestamp(index, null);
      return;
    }

    if (persistTimestampInUTC) {
      super.setInstant(p, index, value);
    } else {
      p.setTimestamp(index, Timestamp.from(value));
    }
  }

  @Override
  public Instant getInstant(ResultSet rs, String columnName) throws SQLException {
    if (persistTimestampInUTC) {
      return super.getInstant(rs, columnName);
    } else {
      return Optional.ofNullable(rs.getTimestamp(columnName))
          .map(Timestamp::toInstant)
          .orElse(null);
    }
  }

  @Override
  public String getQueryLimitPart(int limit) {
    return Queries.postgresSqlLimitPart(limit);
  }
}
