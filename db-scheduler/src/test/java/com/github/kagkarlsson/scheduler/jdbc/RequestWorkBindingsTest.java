package com.github.kagkarlsson.scheduler.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.jdbc.PreparedStatementSetter;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RequestWorkBindingsTest {
  static Stream<Arguments> dialects() {
    return Stream.of(
            new PostgreSqlJdbcCustomization(false, true),
            new SqliteJdbcCustomization(),
            new MySQLBaseJdbcCustomization(true),
            new HsqlJdbcCustomization(true),
            new MssqlJdbcCustomization(true),
            new OracleJdbcCustomization(true)) //
        .flatMap(dialect -> Stream.of(Arguments.of(dialect, false), Arguments.of(dialect, true)));
  }

  @ParameterizedTest
  @MethodSource("dialects")
  void binds_insert_and_conflict_values_with_and_without_priority(
      JdbcCustomization dialect, boolean priority) throws Exception {
    var runner = mock(JdbcRunner.class);
    var ps = mock(PreparedStatement.class);
    when(runner.execute(any(String.class), any(PreparedStatementSetter.class)))
        .thenAnswer(
            invocation -> {
              invocation.<PreparedStatementSetter>getArgument(1).setParameters(ps);
              return 1;
            });
    var request =
        new JdbcWorkParams(
            runner,
            "scheduled_tasks",
            new ScheduledTaskInstance(
                new TaskInstance<>("sync", "id", () -> null, 42), Instant.EPOCH),
            new byte[] {1, 2},
            priority);
    dialect.requestWork(request);

    boolean merge =
        dialect instanceof HsqlJdbcCustomization
            || dialect instanceof MssqlJdbcCustomization
            || dialect instanceof OracleJdbcCustomization;
    int insertStart = merge ? 4 : 1;
    if (merge) {
      verify(ps).setBoolean(3, true); // matched: rerun
    }
    verify(ps).setString(insertStart, "sync");
    verify(ps).setString(insertStart + 1, "id");
    verify(ps).setBoolean(insertStart + 4, false); // insert: picked
    verify(ps).setBoolean(insertStart + 5, false); // insert: rerun
    verify(ps).setLong(insertStart + 6, 1L);
    if (priority) {
      verify(ps).setInt(insertStart + 7, 42);
    }
    if (!merge) {
      verify(ps).setBoolean(priority ? 9 : 8, true); // conflict: rerun
    }
  }
}
