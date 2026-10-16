package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.kagkarlsson.jdbc.JdbcRunner;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.exceptions.ExecutionException;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.jdbc.PostgreSqlJdbcCustomization;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.FailureHandler;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.Task;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.CoalescingTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import com.github.kagkarlsson.scheduler.testhelper.SettableClock;
import com.github.kagkarlsson.scheduler.testhelper.TestHelper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CoalescingTaskTest {
  @RegisterExtension final EmbeddedPostgresqlExtension db = new EmbeddedPostgresqlExtension();
  private final Instant now = Instant.parse("2026-10-16T10:00:00Z");
  private final CoalescingTask<String> task =
      Tasks.coalescing("sync", String.class).execute((i, c) -> {});
  private final TaskInstance<String> instance =
      task.instanceBuilder("account") //
          .data("original") //
          .priority(42) //
          .build();

  enum Claim {
    FETCH,
    GENERIC,
    SINGLE_STATEMENT
  }

  private SettableClock clock() {
    var clock = new SettableClock();
    clock.set(now);
    return clock;
  }

  private JdbcTaskRepository repository(boolean enabled, Task<?>... tasks) {
    var clock = clock();
    return new JdbcTaskRepository(
        db.getDataSource(),
        true,
        new PostgreSqlJdbcCustomization(false, false),
        "scheduled_tasks",
        new TaskResolver(SchedulerListeners.NOOP, clock, List.of(tasks)),
        new SchedulerName.Fixed("test"),
        Serializer.DEFAULT_JAVA_SERIALIZER,
        true,
        enabled,
        clock);
  }

  private Execution claim(JdbcTaskRepository repository, Claim strategy) {
    return switch (strategy) {
      case FETCH -> repository.pick(repository.getDue(now, 1).get(0), now).orElseThrow();
      case GENERIC -> repository.lockAndFetchGeneric(now, 1).get(0);
      case SINGLE_STATEMENT -> repository.lockAndGetDue(now, 1).get(0);
    };
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void requests_before_claim_share_one_execution_and_retain_data(Claim strategy) {
    var repository = repository(true, task);
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    repository.requestWork(new ScheduledTaskInstance(task.instance("account", "ignored"), now));
    var picked = claim(repository, strategy);
    assertThat(picked.rerun).isFalse();
    assertThat(picked.taskInstance.getData()).isEqualTo("original");
    assertThat(picked.taskInstance.getPriority()).isEqualTo(42);
    assertThat(repository.completeCoalescing(picked, now)).isFalse();
    assertThat(repository.getExecution(instance)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void requests_during_each_execution_cause_one_follow_up(Claim strategy) {
    var repository = repository(true, task);
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    for (int run = 0; run < 2; run++) {
      var picked = claim(repository, strategy);
      for (int request = 0; request < 5; request++) {
        repository.requestWork(new ScheduledTaskInstance(task.instance("account", "ignored"), now));
      }
      assertThat(repository.getExecution(instance).orElseThrow().version).isEqualTo(picked.version);
      assertThat(repository.updateHeartbeat(picked, now.plusSeconds(1))).isTrue();
      assertThat(repository.completeCoalescing(picked, now)).isTrue();
      var pending = repository.getExecution(instance).orElseThrow();
      assertThat(pending.picked).isFalse();
      assertThat(pending.lastHeartbeat).isNull();
      assertThat(pending.lastSuccess).isEqualTo(now);
      assertThat(pending.consecutiveFailures).isZero();
      assertThat(pending.taskInstance.getData()).isEqualTo("original");
      assertThat(pending.taskInstance.getPriority()).isEqualTo(42);
    }
    assertThat(repository.completeCoalescing(claim(repository, strategy), now)).isFalse();
    assertThat(repository.getExecution(instance)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void retries_and_dead_execution_recovery_consume_earlier_requests_on_claim(Claim strategy) {
    var repository = repository(true, task);
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    for (int retry = 0; retry < 2; retry++) {
      var picked = claim(repository, strategy);
      repository.requestWork(new ScheduledTaskInstance(instance, now));
      repository.reschedule(picked, now, picked.lastSuccess, now, retry + 1);
      assertThat(repository.getExecution(instance).orElseThrow().rerun).isTrue();
    }
    var retry = claim(repository, strategy);
    assertThat(retry.rerun).isFalse();
    assertThat(repository.completeCoalescing(retry, now)).isFalse();
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void stale_worker_cannot_complete_or_discard_a_new_owners_requests(Claim strategy) {
    var repository = repository(true, task);
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    var stale = claim(repository, strategy);
    repository.reschedule(stale, now, null, now, 1);
    var owner = claim(repository, strategy);
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    assertThatThrownBy(() -> repository.completeCoalescing(stale, now))
        .isInstanceOf(ExecutionException.class);
    assertThat(repository.getExecution(instance).orElseThrow().rerun).isTrue();
    assertThat(repository.completeCoalescing(owner, now)).isTrue();
  }

  @Test
  void ordinary_scheduling_keeps_existing_semantics() {
    var ordinary = Tasks.oneTime("ordinary").execute((i, c) -> {});
    var repository = repository(true, task, ordinary);
    var ordinaryInstance = new ScheduledTaskInstance(ordinary.instance("id"), now);
    assertThat(repository.createIfNotExists(ordinaryInstance)).isTrue();
    assertThat(repository.createIfNotExists(ordinaryInstance)).isFalse();
    assertThat(readRerun(ordinary.instance("id"))).isNull();
    var unknownClientRepository = repository(true);
    unknownClientRepository.requestWork(ordinaryInstance);
    assertThat(readRerun(ordinary.instance("id"))).isTrue();
    repository.remove(repository.getExecution("ordinary", "id").orElseThrow());

    var scheduled = new ScheduledTaskInstance(instance, now);
    assertThat(repository.createIfNotExists(scheduled)).isTrue();
    var picked = claim(repository, Claim.FETCH);
    assertThat(repository.createIfNotExists(scheduled)).isFalse();
    assertThat(repository.getExecution(instance).orElseThrow().rerun).isFalse();
    assertThat(repository.completeCoalescing(picked, now)).isFalse();
  }

  @Test
  void batch_and_replace_initialize_without_task_type_markers() {
    var ordinary = Tasks.oneTime("ordinary").execute((i, c) -> {});
    var repository = repository(true, task, ordinary);
    repository.createBatch(List.of(new ScheduledTaskInstance(instance, now)));
    assertThat(repository.getExecution(instance).orElseThrow().rerun).isFalse();
    repository.replace(
        repository.getExecution(instance).orElseThrow(),
        new ScheduledTaskInstance(ordinary.instance("id"), now));
    assertThat(repository.getExecution("ordinary", "id").orElseThrow().rerun).isFalse();
    repository.replace(
        repository.getExecution("ordinary", "id").orElseThrow(),
        new ScheduledTaskInstance(instance, now));
    assertThat(repository.getExecution(instance).orElseThrow().rerun).isFalse();
  }

  @Test
  void standalone_client_and_restart_retain_pending_work() {
    SchedulerClient client =
        SchedulerClient.Builder.create(db.getDataSource()).enableCoalescing().build();
    client.requestWork(instance);
    var repository = repository(true, task);
    var picked =
        repository.pick(repository.getExecution(instance).orElseThrow(), now).orElseThrow();
    client.requestWork(task.instance("account", "ignored"));
    assertThat(repository.completeCoalescing(picked, now)).isTrue();
    var restarted = repository(true, task);
    assertThat(restarted.completeCoalescing(claim(restarted, Claim.SINGLE_STATEMENT), now))
        .isFalse();
  }

  @Test
  void coalescing_requires_opt_in_and_rejects_known_ordinary_tasks() {
    assertThatThrownBy(() -> Scheduler.create(db.getDataSource(), task).build())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> SchedulerClient.Builder.create(db.getDataSource(), task).build())
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () -> repository(false).requestWork(new ScheduledTaskInstance(instance, now)))
        .isInstanceOf(IllegalStateException.class);
    var ordinary = Tasks.oneTime("ordinary").execute((i, c) -> {});
    var client =
        SchedulerClient.Builder.create(db.getDataSource(), ordinary).enableCoalescing().build();
    assertThatThrownBy(() -> client.requestWork(ordinary.instance("id")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void disabled_support_works_without_the_column() {
    var sql = new JdbcRunner(db.getDataSource());
    sql.execute("create table legacy_tasks (like scheduled_tasks including all)", ps -> {});
    try {
      sql.execute("alter table legacy_tasks drop column rerun", ps -> {});
      var ordinary = Tasks.oneTime("ordinary").execute((i, c) -> {});
      var scheduler =
          TestHelper.createManualScheduler(db.getDataSource(), ordinary) //
              .clock(clock()) //
              .tableName("legacy_tasks")
              .build();
      scheduler.scheduleIfNotExists(ordinary.instance("id"), now);
      scheduler.runAnyDueExecutions();
      assertThat(scheduler.getScheduledExecution(ordinary.instance("id"))).isEmpty();
    } finally {
      sql.execute("drop table legacy_tasks", ps -> {});
    }
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void real_scheduler_runs_follow_up_and_uses_configured_retry_handler(Claim strategy) {
    var calls = new AtomicInteger();
    var clients = new SchedulerClient[1];
    var actualTask =
        Tasks.coalescing("actual", String.class) //
            .onFailure(new FailureHandler.OnFailureRetryLater<>(Duration.ofSeconds(1))) //
            .execute(
                (i, c) -> {
                  assertThat(i.getData()).isEqualTo("original");
                  int call = calls.incrementAndGet();
                  if (call == 1 || call == 2) {
                    clients[0].requestWork(i);
                    clients[0].requestWork(i);
                  }
                  if (call == 1) {
                    throw new RuntimeException("retry me");
                  }
                });
    var builder =
        TestHelper.createManualScheduler(db.getDataSource(), actualTask) //
            .clock(clock())
            .enableCoalescing();
    if (strategy != Claim.FETCH) {
      builder.jdbcCustomization(new PostgreSqlJdbcCustomization(strategy == Claim.GENERIC, false));
      builder.pollingStrategy(
          new PollingStrategyConfig(PollingStrategyConfig.Type.LOCK_AND_FETCH, 0.5, 1));
    }
    var scheduler = builder.build();
    clients[0] = scheduler;
    scheduler.requestWork(actualTask.instance("account", "original"));
    scheduler.runAnyDueExecutions();
    assertThat(calls.get()).isEqualTo(1);
    scheduler.tick(Duration.ofSeconds(1));
    scheduler.runAnyDueExecutions();
    assertThat(calls.get()).isEqualTo(2);
    scheduler.runAnyDueExecutions();
    assertThat(calls.get()).isEqualTo(3);
    assertThat(scheduler.getScheduledExecution(actualTask.instance("account"))).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void request_racing_with_completion_is_never_lost(Claim strategy) throws Exception {
    var repository = repository(true, task);
    var executor = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < 100; iteration++) {
        repository.requestWork(new ScheduledTaskInstance(instance, now));
        var picked = claim(repository, strategy);
        var start = new CountDownLatch(1);
        var request =
            executor.submit(
                () -> {
                  await(start);
                  repository.requestWork(new ScheduledTaskInstance(instance, now));
                });
        var complete =
            executor.submit(
                () -> {
                  await(start);
                  repository.completeCoalescing(picked, now);
                });
        start.countDown();
        request.get(10, TimeUnit.SECONDS);
        complete.get(10, TimeUnit.SECONDS);
        assertThat(repository.getExecution(instance).orElseThrow().picked).isFalse();
        assertThat(repository.completeCoalescing(claim(repository, strategy), now)).isFalse();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void concurrent_first_requests_create_one_execution() throws Exception {
    var repository = repository(true, task);
    var executor = Executors.newFixedThreadPool(10);
    try {
      var start = new CountDownLatch(1);
      var requests = new ArrayList<Future<?>>();
      for (int i = 0; i < 10; i++) {
        requests.add(
            executor.submit(
                () -> {
                  await(start);
                  repository.requestWork(new ScheduledTaskInstance(instance, now));
                }));
      }
      start.countDown();
      for (var request : requests) {
        request.get(10, TimeUnit.SECONDS);
      }
      assertThat(repository.completeCoalescing(claim(repository, Claim.FETCH), now)).isFalse();
      assertThat(repository.getExecution(instance)).isEmpty();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void terminal_failure_discards_pending_work_and_a_later_request_can_start_again() {
    var clients = new SchedulerClient[1];
    var task =
        Tasks.coalescing("terminal") //
            .onFailure((completed, operations) -> operations.stop()) //
            .execute(
                (i, c) -> {
                  clients[0].requestWork(i);
                  throw new RuntimeException("terminal failure");
                });
    var scheduler =
        TestHelper.createManualScheduler(db.getDataSource(), task) //
            .clock(clock()) //
            .enableCoalescing()
            .build();
    clients[0] = scheduler;
    scheduler.requestWork(task.instance("id"));
    scheduler.runAnyDueExecutions();
    assertThat(scheduler.getScheduledExecution(task.instance("id"))).isEmpty();
    scheduler.requestWork(task.instance("id"));
    assertThat(scheduler.getScheduledExecution(task.instance("id"))).isPresent();
    scheduler.cancel(task.instance("id"));
    assertThat(scheduler.getScheduledExecution(task.instance("id"))).isEmpty();
  }

  @Test
  void dead_execution_recovery_runs_once_and_consumes_pending_requests() {
    var calls = new AtomicInteger();
    var deadTask = Tasks.coalescing("dead").execute((i, c) -> calls.incrementAndGet());
    var scheduler =
        TestHelper.createManualScheduler(db.getDataSource(), deadTask) //
            .clock(clock()) //
            .enableCoalescing()
            .build();
    var repository = repository(true, deadTask);
    var instance = deadTask.instance("id");
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    claim(repository, Claim.FETCH); // Simulate an owner dying after claiming the task.
    scheduler.requestWork(instance);
    scheduler.tick(Duration.ofHours(1));
    scheduler.runDeadExecutionDetection();
    var revived = repository.getExecution(instance).orElseThrow();
    assertThat(revived.rerun).isTrue();
    // The existing ReviveDeadExecution handler uses Instant.now(), rather than the scheduler clock.
    scheduler.setTime(revived.executionTime);
    scheduler.runAnyDueExecutions();
    assertThat(calls.get()).isEqualTo(1);
    assertThat(scheduler.getScheduledExecution(instance)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void standalone_scheduling_without_definitions_runs_without_requesting_extra_work(
      Claim strategy) {
    var client = SchedulerClient.Builder.create(db.getDataSource()).enablePriority().build();
    assertThat(client.scheduleIfNotExists(instance, now)).isTrue();
    assertThat(readRerun(instance)).isNull();
    var repository = repository(true, task);
    var picked = claim(repository, strategy);
    assertThat(picked.rerun).isFalse();
    assertThat(readRerun(instance)).isFalse();
    assertThat(repository.completeCoalescing(picked, now)).isFalse();
    assertThat(repository.getExecution(instance)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Claim.class)
  void request_upgrades_null_before_claim_and_is_consumed_by_that_execution(Claim strategy) {
    var client =
        SchedulerClient.Builder.create(db.getDataSource()) //
            .enablePriority() //
            .enableCoalescing() //
            .build();
    client.scheduleIfNotExists(instance, now);
    assertThat(readRerun(instance)).isNull();
    client.requestWork(task.instance("account", "ignored"));
    assertThat(readRerun(instance)).isTrue();
    var repository = repository(true, task);
    var picked = claim(repository, strategy);
    assertThat(picked.rerun).isFalse();
    assertThat(picked.taskInstance.getData()).isEqualTo("original");
    assertThat(picked.taskInstance.getPriority()).isEqualTo(42);
    assertThat(repository.completeCoalescing(picked, now)).isFalse();
    assertThat(repository.getExecution(instance)).isEmpty();
  }

  @Test
  void completion_accepts_a_null_flag_and_requests_upgrade_it_during_execution() {
    var client =
        SchedulerClient.Builder.create(db.getDataSource()) //
            .enablePriority() //
            .enableCoalescing() //
            .build();
    var repository = repository(true, task);
    var legacy = repository(false, task);
    for (boolean requestDuringExecution : List.of(false, true)) {
      client.scheduleIfNotExists(instance, now);
      // A claim without coalescing support leaves NULL in the database. Completion must handle it
      // identically to false; no task type may be inferred from the column.
      var picked = legacy.pick(legacy.getExecution(instance).orElseThrow(), now).orElseThrow();
      assertThat(readRerun(instance)).isNull();
      if (requestDuringExecution) {
        client.requestWork(task.instance("account", "ignored"));
        assertThat(readRerun(instance)).isTrue();
      }
      assertThat(repository.completeCoalescing(picked, now)).isEqualTo(requestDuringExecution);
      if (requestDuringExecution) {
        var followUp = claim(repository, Claim.FETCH);
        assertThat(followUp.taskInstance.getData()).isEqualTo("original");
        assertThat(repository.completeCoalescing(followUp, now)).isFalse();
      }
      assertThat(repository.getExecution(instance)).isEmpty();
    }
  }

  private Boolean readRerun(TaskInstance<?> instance) {
    return new JdbcRunner(db.getDataSource())
        .query(
            "select rerun from scheduled_tasks where task_name = ? and task_instance = ?",
            ps -> {
              ps.setString(1, instance.getTaskName());
              ps.setString(2, instance.getId());
            },
            (com.github.kagkarlsson.jdbc.ResultSetMapper<Boolean>)
                rs -> {
                  assertThat(rs.next()).isTrue();
                  boolean rerun = rs.getBoolean("rerun");
                  return rs.wasNull() ? null : rerun;
                });
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting for concurrent operation");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
