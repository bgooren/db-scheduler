package com.github.kagkarlsson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.kagkarlsson.jdbc.HsqlExtension;
import com.github.kagkarlsson.scheduler.event.SchedulerListeners;
import com.github.kagkarlsson.scheduler.jdbc.HsqlJdbcCustomization;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import com.github.kagkarlsson.scheduler.task.ScheduledTaskInstance;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class CoalescingHsqlTest {
  @RegisterExtension final HsqlExtension db = new HsqlExtension();

  @Test
  void hsql_supports_upserts_and_null_flags() {
    DbUtils.runSqlResource("/hsql_tables.sql").accept(db.getDataSource());
    var task = Tasks.coalescing("sync").execute((i, c) -> {});
    var ordinary = Tasks.oneTime("ordinary").execute((i, c) -> {});
    var clock = new SystemClock();
    var repository =
        new JdbcTaskRepository(
            db.getDataSource(),
            true,
            new HsqlJdbcCustomization(false),
            "scheduled_tasks",
            new TaskResolver(SchedulerListeners.NOOP, clock, List.of(task, ordinary)),
            new SchedulerName.Fixed("hsql"),
            Serializer.DEFAULT_JAVA_SERIALIZER,
            false,
            true,
            clock);
    var instance = task.instance("id");
    var now = Instant.now();
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    var picked =
        repository.pick(repository.getExecution(instance).orElseThrow(), now).orElseThrow();
    repository.requestWork(new ScheduledTaskInstance(instance, now));
    assertThat(repository.completeCoalescing(picked, now)).isTrue();
    var followUp =
        repository.pick(repository.getExecution(instance).orElseThrow(), now).orElseThrow();
    assertThat(followUp.rerun).isFalse();
    assertThat(repository.completeCoalescing(followUp, now)).isFalse();
    assertThat(repository.getExecution(instance)).isEmpty();
    repository.createIfNotExists(new ScheduledTaskInstance(ordinary.instance("id"), now));
    assertThat(repository.getExecution(ordinary.instance("id")).orElseThrow().rerun).isFalse();
    var standalone =
        SchedulerClient.Builder.create(db.getDataSource()) //
            .enableCoalescing()
            .build();
    standalone.requestWork(ordinary.instance("id"));
    assertThat(repository.getExecution(ordinary.instance("id")).orElseThrow().rerun).isTrue();
  }

  @Test
  void concurrent_requests_use_hsql_merge() throws Exception {
    DbUtils.runSqlResource("/hsql_tables.sql").accept(db.getDataSource());
    var task = Tasks.coalescing("concurrent").execute((i, c) -> {});
    var client =
        SchedulerClient.Builder.create(db.getDataSource(), task) //
            .enableCoalescing()
            .build();
    var executor = java.util.concurrent.Executors.newFixedThreadPool(8);
    try {
      var start = new java.util.concurrent.CountDownLatch(1);
      var requests = new java.util.ArrayList<java.util.concurrent.Future<?>>();
      for (int i = 0; i < 8; i++) {
        requests.add(
            executor.submit(
                () -> {
                  try {
                    start.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                  client.requestWork(task.instance("same-id"));
                }));
      }
      start.countDown();
      for (var request : requests) {
        request.get(10, java.util.concurrent.TimeUnit.SECONDS);
      }
      assertThat(client.getScheduledExecution(task.instance("same-id"))).isPresent();
    } finally {
      executor.shutdownNow();
    }
  }
}
