package com.github.kagkarlsson.scheduler.jdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import com.github.kagkarlsson.jdbc.SQLRuntimeException;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OracleRequestWorkTest {
  @Mock JdbcRunner runner;

  private JdbcWorkParams request() {
    return new JdbcWorkParams(
        runner,
        "scheduled_tasks",
        new ScheduledTaskInstance(new TaskInstance<>("sync", "id"), Instant.EPOCH),
        new byte[0],
        false);
  }

  @Test
  void concurrent_insert_followed_by_completion_requires_transaction_retry() {
    var duplicate = new SQLRuntimeException(new SQLException("duplicate key", "23000", 1));
    when(runner.execute(startsWith("merge into"), any(PreparedStatementSetter.class)))
        .thenThrow(duplicate);
    when(runner.execute(startsWith("update"), any(PreparedStatementSetter.class))).thenReturn(0);
    assertThatThrownBy(() -> new OracleJdbcCustomization(false).requestWork(request())) //
        .isInstanceOf(SQLRuntimeException.class) //
        .hasCauseInstanceOf(SQLTransientException.class);
    verify(runner).execute(startsWith("merge into"), any(PreparedStatementSetter.class));
    verify(runner).execute(startsWith("update"), any(PreparedStatementSetter.class));
  }

  @Test
  void concurrent_insert_updates_the_winners_row_once() {
    var duplicate = new SQLRuntimeException(new SQLException("duplicate key", "23000", 1));
    when(runner.execute(startsWith("merge into"), any(PreparedStatementSetter.class)))
        .thenThrow(duplicate);
    when(runner.execute(startsWith("update"), any(PreparedStatementSetter.class))).thenReturn(1);
    new OracleJdbcCustomization(false).requestWork(request());
    verify(runner).execute(startsWith("merge into"), any(PreparedStatementSetter.class));
    verify(runner).execute(startsWith("update"), any(PreparedStatementSetter.class));
    verifyNoMoreInteractions(runner);
  }

  @Test
  void unrelated_sql_error_is_propagated_without_retry() {
    var missingTable = new SQLRuntimeException(new SQLException("missing table", "42000", 942));
    when(runner.execute(startsWith("merge into"), any(PreparedStatementSetter.class)))
        .thenThrow(missingTable);
    assertThatThrownBy(() -> new OracleJdbcCustomization(false).requestWork(request()))
        .isSameAs(missingTable);
    verify(runner, never()).execute(startsWith("update"), any(PreparedStatementSetter.class));
  }
}
