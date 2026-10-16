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
package com.github.kagkarlsson.scheduler.task.helper;

import com.github.kagkarlsson.scheduler.task.*;

/**
 * A one-time task whose requests are coalesced by task name and instance ID. Requests made during
 * an execution cause one follow-up execution. Requires coalescing support and the nullable rerun
 * column. Existing task data is retained when requesting more work.
 */
public abstract class CoalescingTask<T> extends OneTimeTask<T> {

  public CoalescingTask(String name, Class<T> dataClass) {
    super(name, dataClass);
  }

  public CoalescingTask(String name, Class<T> dataClass, FailureHandler<T> failureHandler) {
    super(name, dataClass, failureHandler);
  }

  public CoalescingTask(
      String name,
      Class<T> dataClass,
      FailureHandler<T> failureHandler,
      DeadExecutionHandler<T> deadExecutionHandler) {
    super(name, dataClass, failureHandler, deadExecutionHandler);
  }

  public CoalescingTask(
      String name,
      Class<T> dataClass,
      FailureHandler<T> failureHandler,
      DeadExecutionHandler<T> deadExecutionHandler,
      int defaultPriority) {
    super(name, dataClass, failureHandler, deadExecutionHandler, defaultPriority);
  }

  @Override
  public final CompletionHandler<T> execute(
      TaskInstance<T> taskInstance, ExecutionContext context) {
    executeOnce(taskInstance, context);
    return (completed, operations) -> operations.completeCoalescing(completed);
  }

  @Override
  public String toString() {
    return "CoalescingTask name=" + getName();
  }
}
