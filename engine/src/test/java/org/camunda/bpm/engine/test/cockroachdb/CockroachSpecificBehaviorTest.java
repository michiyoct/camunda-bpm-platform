/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. Camunda licenses this file to you under the Apache License,
 * Version 2.0; you may not use this file except in compliance with the License.
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
package org.camunda.bpm.engine.test.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.camunda.bpm.engine.CrdbTransactionRetryException;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.OptimisticLockingException;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.ProcessEngines;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.BootstrapEngineCommand;
import org.camunda.bpm.engine.impl.ProcessEngineLogger;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.cmd.AcquireJobsCmd;
import org.camunda.bpm.engine.impl.cmd.ExecuteJobsCmd;
import org.camunda.bpm.engine.impl.cmd.HistoryCleanupCmd;
import org.camunda.bpm.engine.impl.cmd.SetJobDefinitionPriorityCmd;
import org.camunda.bpm.engine.impl.cmd.SuspendJobCmd;
import org.camunda.bpm.engine.impl.cmd.SuspendJobDefinitionCmd;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.sql.DbSqlSessionFactory;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.interceptor.CommandInvocationContext;
import org.camunda.bpm.engine.impl.jobexecutor.AcquiredJobs;
import org.camunda.bpm.engine.impl.jobexecutor.ExecuteJobHelper;
import org.camunda.bpm.engine.impl.jobexecutor.JobExecutor;
import org.camunda.bpm.engine.impl.jobexecutor.JobFailureCollector;
import org.camunda.bpm.engine.impl.management.UpdateJobDefinitionSuspensionStateBuilderImpl;
import org.camunda.bpm.engine.impl.management.UpdateJobSuspensionStateBuilderImpl;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.test.RequiredDatabase;
import org.camunda.bpm.engine.management.JobDefinition;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.test.concurrency.ConcurrencyTestHelper;
import org.camunda.bpm.engine.test.concurrency.ControllableThread;
import org.camunda.bpm.engine.test.concurrency.ControlledCommand;
import org.camunda.bpm.engine.test.jobexecutor.ControllableJobExecutor;
import org.camunda.bpm.engine.test.util.ProcessEngineBootstrapRule;
import org.camunda.bpm.engine.test.util.ProcessEngineTestRule;
import org.camunda.bpm.engine.test.util.ProvidedProcessEngineRule;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;

/**
 * Let's add CRDB-specific behavior tests here
 */
@RequiredDatabase(includes = DbSqlSessionFactory.CRDB)
public class CockroachSpecificBehaviorTest extends ConcurrencyTestHelper {

  protected static final Logger LOG = ProcessEngineLogger.TEST_LOGGER.getLogger();
  protected static final int COMMAND_RETRIES = 3;
  protected static final String PROCESS_ENGINE_NAME = "retriableBootstrapProcessEngine";
  protected static final BpmnModelInstance SIMPLE_ASYNC_PROCESS = Bpmn.createExecutableProcess("simpleAsyncProcess")
      .startEvent()
      .serviceTask()
        .camundaExpression("${true}")
        .camundaAsyncBefore()
      .endEvent()
      .done();

  @ClassRule
  public static ProcessEngineBootstrapRule bootstrapRule = new ProcessEngineBootstrapRule(
    c -> c.setCommandRetries(COMMAND_RETRIES).setJobExecutor(new ControllableJobExecutor()));
  protected ProvidedProcessEngineRule engineRule = new ProvidedProcessEngineRule(bootstrapRule);
  protected ProcessEngineTestRule testRule = new ProcessEngineTestRule(engineRule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(engineRule).around(testRule);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected static ControllableThread activeThread;

  protected ProcessEngine processEngine;
  protected RuntimeService runtimeService;
  protected ManagementService managementService;
  protected HistoryService historyService;
  protected RepositoryService repositoryService;

  @Before
  public void setUp() {
    processEngine = engineRule.getProcessEngine();
    processEngineConfiguration = engineRule.getProcessEngineConfiguration();
    managementService = engineRule.getManagementService();
    runtimeService = engineRule.getRuntimeService();
    historyService = engineRule.getHistoryService();
    repositoryService = engineRule.getRepositoryService();
  }

  @After
  public void tearDown() throws Exception {
    testRule.deleteHistoryCleanupJobs();
    processEngineConfiguration.getCommandExecutorTxRequired().execute((Command<Void>) commandContext -> {

      commandContext.getMeterLogManager().deleteAll();
      List<Job> jobs = processEngine.getManagementService().createJobQuery().list();
      if (jobs.size() > 0) {
        String jobId = jobs.get(0).getId();
        commandContext.getJobManager().deleteJob((JobEntity) jobs.get(0));
        commandContext.getHistoricJobLogManager().deleteHistoricJobLogByJobId(jobId);
      }
      commandContext.getHistoricJobLogManager().deleteHistoricJobLogsByHandlerType("history-cleanup");

      return null;
    });
    closeDownProcessEngine();
  }

  @Test
  public void shouldRetryTxToBootstrapConcurrentProcessEngine() throws InterruptedException {

    processEngine.getHistoryService().cleanUpHistoryAsync(true);

    ThreadControl thread1 = executeControllableCommand(new ControllableJobExecutionCommand());
    thread1.reportInterrupts();
    thread1.waitForSync();

    ControllableProcessEngineBootstrapCommand bootstrapCommand = new ControllableProcessEngineBootstrapCommand();
    ThreadControl thread2 = executeControllableCommand(bootstrapCommand);
    thread2.reportInterrupts();
    thread2.waitForSync();

    thread1.makeContinue();
    thread1.waitForSync();

    thread2.makeContinue();

    Thread.sleep(2000);

    thread1.waitUntilDone();

    thread2.waitForSync();
    thread2.waitUntilDone(true);

    assertNull(thread1.getException());
    assertNull(thread2.getException());

    // When CockroachDB is used, the CrdbTransactionRetryException is caught by
    // the CrdbTransactionRetryInterceptor and the command is retried
    assertThat(bootstrapCommand.getContextSpy().getThrowable()).isNull();
    assertThat(bootstrapCommand.getTries()).isEqualTo(2);

    // the Process Engine is successfully registered even when run on CRDB
    // since the OLE is caught and handled during the Process Engine Bootstrap command
    assertNotNull(ProcessEngines.getProcessEngines().get(PROCESS_ENGINE_NAME));
  }

  @Test
  public void shouldRethrowBootstrapEngineOleWhenRetriesAreExausted() {
    // given
    // a bootstrap command failing with a CrdbTransactionRetryException
    FailingProcessEngineBootstrapCommand bootstrapCommand = new FailingProcessEngineBootstrapCommand();
    ProcessEngineConfigurationImpl processEngineConfiguration = ((ProcessEngineConfigurationImpl)
      ProcessEngineConfiguration
        .createProcessEngineConfigurationFromResource("camunda.cfg.xml"))
      .setCommandRetries(COMMAND_RETRIES)
      .setProcessEngineName(PROCESS_ENGINE_NAME);
    processEngineConfiguration.setProcessEngineBootstrapCommand(bootstrapCommand);

    // then
    // a CrdbTransactionRetryException is re-thrown to the caller
    thrown.expect(CrdbTransactionRetryException.class);

    // when
    processEngineConfiguration.buildProcessEngine();

    // since the Command retries were exausted
    assertThat(bootstrapCommand.getTries()).isEqualTo(4);
  }

  @Ignore("TODO: adjust for retries")
  @Test
  public void shouldRetryAcquistionJobTxAfterJobSuspensionOLE() {
    testRule.deploy(SIMPLE_ASYNC_PROCESS);

    runtimeService.startProcessInstanceByKey("simpleAsyncProcess");

    // given a waiting acquisition and a waiting suspension
    JobAcquisitionThread acquisitionThread = new JobAcquisitionThread();
    acquisitionThread.startAndWaitUntilControlIsReturned();

    JobSuspensionThread jobSuspensionThread = new JobSuspensionThread("simpleAsyncProcess");
    jobSuspensionThread.startAndWaitUntilControlIsReturned();

    // first complete suspension:
    jobSuspensionThread.proceedAndWaitTillDone();
    acquisitionThread.proceedAndWaitTillDone();

    // then the acquisition will not fail with optimistic locking
    assertNull(jobSuspensionThread.exception);

    if (testRule.databaseSupportsIgnoredOLE()) {
      assertNull(acquisitionThread.exception);
      // but the job will also not be acquired
      assertEquals(0, acquisitionThread.acquiredJobs.size());
    } else {
      // on CockroachDB, the TX of the acquisition thread
      // will fail with an un-ignorable OLE and needs to be retried
      assertThat(acquisitionThread.exception).isInstanceOf(OptimisticLockingException.class);
      // and no result will be returned
      assertNull(acquisitionThread.acquiredJobs);
    }

    //--------------------------------------------

    // given a waiting acquisition and a waiting suspension
    acquisitionThread = new JobAcquisitionThread();
    acquisitionThread.startAndWaitUntilControlIsReturned();

    jobSuspensionThread = new JobSuspensionThread("simpleAsyncProcess");
    jobSuspensionThread.startAndWaitUntilControlIsReturned();

    // first complete acquisition:
    acquisitionThread.proceedAndWaitTillDone();
    jobSuspensionThread.proceedAndWaitTillDone();

    // then there are no optimistic locking exceptions
    assertNull(jobSuspensionThread.exception);
    assertNull(acquisitionThread.exception);
  }

  @Ignore("TODO: adjust for retries")
  @Test
  public void shouldRetryJobExecutionTxAfterJobPriorityOLE() {
    testRule.deploy(SIMPLE_ASYNC_PROCESS);

    // given
    // two running instances
    runtimeService.startProcessInstanceByKey("simpleAsyncProcess");
    runtimeService.startProcessInstanceByKey("simpleAsyncProcess");

    // and a job definition
    JobDefinition jobDefinition = managementService.createJobDefinitionQuery().singleResult();

    // and two jobs
    List<Job> jobs = managementService.createJobQuery().list();

    // when the first job is executed but has not yet committed
    JobExecutionThread executionThread = new JobExecutionThread(jobs.get(0).getId());
    executionThread.startAndWaitUntilControlIsReturned();

    // and the job priority is updated
    JobDefinitionPriorityThread priorityThread = new JobDefinitionPriorityThread(jobDefinition.getId(), 42L, true);
    priorityThread.startAndWaitUntilControlIsReturned();

    // and the priority threads commits first
    priorityThread.proceedAndWaitTillDone();

    // then both jobs priority has changed
    List<Job> currentJobs = managementService.createJobQuery().list();
    for (Job job : currentJobs) {
      assertEquals(42, job.getPriority());
    }

    // and the execution thread can nevertheless successfully finish job execution
    executionThread.proceedAndWaitTillDone();

    long remainingJobCount = managementService.createJobQuery().count();
    if (testRule.databaseSupportsIgnoredOLE()) {
      assertNull(executionThread.exception);

      // and ultimately only one job with an updated priority is left
      assertEquals(1L, remainingJobCount);
    } else {
      // on CockroachDB, the TX of the execution thread
      // will fail with an un-ignorable OLE and needs to be retried
      assertThat(executionThread.exception).isInstanceOf(OptimisticLockingException.class);
      // and both jobs will remain available
      assertEquals(2L, remainingJobCount);
    }
  }

  @Test
  public void testRunTwoHistoryCleanups() throws InterruptedException {
    // given
    // first thread that executes a HistoryCleanupCmd
    ThreadControl thread1 = executeControllableCommand(new ControllableHistoryCleanupCommand());
    thread1.waitForSync();

    // second thread that executes a HistoryCleanupCmd
    ThreadControl thread2 = executeControllableCommand(new ControllableHistoryCleanupCommand());
    thread2.reportInterrupts();
    thread2.waitForSync();

    // first thread executes the job, reconfigures the next one and waits to flush to the db
    thread1.makeContinue();
    thread1.waitForSync();

    // second thread executes the job, reconfigures the next one and waits to flush to the db
    thread2.makeContinue();

    Thread.sleep(2000);

    // first thread flushes the changes to the db
    thread1.waitUntilDone();

    //only one history cleanup job exists -> no exception
    List<Job> historyCleanupJobs = processEngine.getHistoryService().findHistoryCleanupJobs();
    assertEquals(1, historyCleanupJobs.size());
    Job firstHistoryCleanupJob = historyCleanupJobs.get(0);

    // second thread attempts to flush, fails and retries
    thread2.waitForSync();
    thread2.waitUntilDone(true);

    // the OLE was caught by the CrdbTransactionRetryInterceptor
    assertNull(thread2.getException());
    // and the command was retried
    assertEquals(2, ((ControllableHistoryCleanupCommand)controllableCommands.get(1)).getRetries());

    //still, only one history cleanup job exists -> no exception
    historyCleanupJobs = processEngine.getHistoryService().findHistoryCleanupJobs();
    assertEquals(1, historyCleanupJobs.size());

    // however, thread2 successfully reconfigured the HistoryCleanupJob
    Job secondHistoryCleanupJob = historyCleanupJobs.get(0);
    assertTrue(secondHistoryCleanupJob.getDuedate().after(firstHistoryCleanupJob.getDuedate()));
  }

  protected static class ControllableProcessEngineBootstrapCommand extends ControllableCommand<Void> {

    protected ControllableBootstrapEngineCommand bootstrapCommand;

    public ControllableProcessEngineBootstrapCommand() {
      this.bootstrapCommand = new ControllableBootstrapEngineCommand(this.monitor);
    }

    @Override
    public Void execute(CommandContext commandContext) {

      ProcessEngineConfigurationImpl processEngineConfiguration = ((ProcessEngineConfigurationImpl)
          ProcessEngineConfiguration
              .createProcessEngineConfigurationFromResource("camunda.cfg.xml"))
              .setCommandRetries(COMMAND_RETRIES)
              .setProcessEngineName(PROCESS_ENGINE_NAME);
      processEngineConfiguration.setProcessEngineBootstrapCommand(bootstrapCommand);

      processEngineConfiguration.buildProcessEngine();

      return null;
    }

    public int getTries() {
      return bootstrapCommand.getTries();
    }

    public CommandInvocationContext getContextSpy() {
      return bootstrapCommand.getSpy();
    }
  }

  protected static class ControllableBootstrapEngineCommand extends BootstrapEngineCommand {

    protected final ThreadControl monitor;
    protected CommandInvocationContext spy;
    protected int tries;

    public ControllableBootstrapEngineCommand(ThreadControl threadControl) {
      this.monitor = threadControl;
      this.tries = 0;
    }

    @Override
    protected void createHistoryCleanupJob(CommandContext commandContext) {

      monitor.sync();

      tries++;
      super.createHistoryCleanupJob(commandContext);
      spy = Context.getCommandInvocationContext();

      monitor.sync();
    }

    public int getTries() {
      return tries;
    }

    @Override
    public boolean isRetryable() {
      return super.isRetryable();
    }

    public CommandInvocationContext getSpy() {
      return spy;
    }
  }

  protected static class FailingProcessEngineBootstrapCommand  extends BootstrapEngineCommand {

    protected int tries;

    public FailingProcessEngineBootstrapCommand() {
      this.tries = 0;
    }

    @Override
    public Void execute(CommandContext commandContext) {

      tries++;
      throw new CrdbTransactionRetryException("The Process Engine Bootstrap has failed.");

    }

    public int getTries() {
      return tries;
    }

    @Override
    public boolean isRetryable() {
      return super.isRetryable();
    }
  }

  protected static class ControllableJobExecutionCommand extends ControllableCommand<Void> {

    @Override
    public Void execute(CommandContext commandContext) {

      monitor.sync();

      List<Job> historyCleanupJobs = commandContext.getProcessEngineConfiguration()
          .getHistoryService()
          .findHistoryCleanupJobs();

      for (Job job : historyCleanupJobs) {
        commandContext.getProcessEngineConfiguration().getManagementService().executeJob(job.getId());
      }

      monitor.sync();

      return null;
    }
  }

  protected static class ControllableHistoryCleanupCommand extends ControllableCommand<Void> {

    protected int retries;
    protected HistoryCleanupCmd historyCleanupCmd;

    public ControllableHistoryCleanupCommand() {
      this.retries = 0;
      this.historyCleanupCmd = new HistoryCleanupCmd(true);
    }

    public Void execute(CommandContext commandContext) {
      monitor.sync();  // thread will block here until makeContinue() is called form main thread

      historyCleanupCmd.execute(commandContext);

      // increment command retries;
      retries++;

      monitor.sync();  // thread will block here until waitUntilDone() is called form main thread

      return null;
    }

    @Override
    public boolean isRetryable() {
      return historyCleanupCmd.isRetryable();
    }

    public int getRetries() {
      return retries;
    }
  }

  public class JobExecutionThread extends ControllableThread {

    OptimisticLockingException exception;
    String jobId;

    JobExecutionThread(String jobId) {
      this.jobId = jobId;
    }

    @Override
    public synchronized void startAndWaitUntilControlIsReturned() {
      activeThread = this;
      super.startAndWaitUntilControlIsReturned();
    }

    @Override
    public void run() {
      try {
        JobFailureCollector jobFailureCollector = new JobFailureCollector(jobId);
        ExecuteJobHelper.executeJob(jobId,
            processEngineConfiguration.getCommandExecutorTxRequired(),
            jobFailureCollector,
            new ControlledCommand<>(activeThread, new ExecuteJobsCmd(jobId, jobFailureCollector)));

      }
      catch (OptimisticLockingException e) {
        this.exception = e;
      }
      LOG.debug(getName() + " ends");
    }
  }

  public class JobAcquisitionThread extends ControllableThread {
    OptimisticLockingException exception;
    AcquiredJobs acquiredJobs;
    @Override
    public synchronized void startAndWaitUntilControlIsReturned() {
      activeThread = this;
      super.startAndWaitUntilControlIsReturned();
    }
    @Override
    public void run() {
      try {
        JobExecutor jobExecutor = processEngineConfiguration.getJobExecutor();
        acquiredJobs = processEngineConfiguration.getCommandExecutorTxRequired()
          .execute(new ControlledCommand<>(activeThread, new AcquireJobsCmd(jobExecutor)));

      } catch (OptimisticLockingException e) {
        this.exception = e;
      }
      LOG.debug(getName()+" ends");
    }
  }

  public class JobSuspensionThread extends ControllableThread {
    OptimisticLockingException exception;
    String processDefinitionKey;

    public JobSuspensionThread(String processDefinitionKey) {
      this.processDefinitionKey = processDefinitionKey;
    }

    @Override
    public synchronized void startAndWaitUntilControlIsReturned() {
      activeThread = this;
      super.startAndWaitUntilControlIsReturned();
    }

    @Override
    public void run() {
      try {
        processEngineConfiguration.getCommandExecutorTxRequired()
          .execute(new ControlledCommand<>(activeThread, createSuspendJobCommand()));

      } catch (OptimisticLockingException e) {
        this.exception = e;
      }
      LOG.debug(getName()+" ends");
    }

    protected Command<Void> createSuspendJobCommand() {
      UpdateJobDefinitionSuspensionStateBuilderImpl builder = new UpdateJobDefinitionSuspensionStateBuilderImpl()
        .byProcessDefinitionKey(processDefinitionKey)
        .includeJobs(true);

      return new SuspendJobDefinitionCmd(builder);
    }
  }

  public class JobSuspensionByJobDefinitionThread extends ControllableThread {
    OptimisticLockingException exception;
    String jobDefinitionId;

    public JobSuspensionByJobDefinitionThread(String jobDefinitionId) {
      this.jobDefinitionId = jobDefinitionId;
    }

    @Override
    public synchronized void startAndWaitUntilControlIsReturned() {
      activeThread = this;
      super.startAndWaitUntilControlIsReturned();
    }

    @Override
    public void run() {
      try {
        processEngineConfiguration.getCommandExecutorTxRequired()
          .execute(new ControlledCommand<>(activeThread, createSuspendJobCommand()));

      } catch (OptimisticLockingException e) {
        this.exception = e;
      }
      LOG.debug(getName()+" ends");
    }

    protected SuspendJobCmd createSuspendJobCommand() {
      UpdateJobSuspensionStateBuilderImpl builder = new UpdateJobSuspensionStateBuilderImpl().byJobDefinitionId(jobDefinitionId);
      return new SuspendJobCmd(builder);
    }
  }

  public class JobDefinitionPriorityThread extends ControllableThread {
    OptimisticLockingException exception;
    String jobDefinitionId;
    Long priority;
    boolean cascade;

    public JobDefinitionPriorityThread(String jobDefinitionId, Long priority, boolean cascade) {
      this.jobDefinitionId = jobDefinitionId;
      this.priority = priority;
      this.cascade = cascade;
    }

    @Override
    public synchronized void startAndWaitUntilControlIsReturned() {
      activeThread = this;
      super.startAndWaitUntilControlIsReturned();
    }

    @Override
    public void run() {
      try {
        processEngineConfiguration.getCommandExecutorTxRequired()
          .execute(new ControlledCommand<>(activeThread, new SetJobDefinitionPriorityCmd(jobDefinitionId, priority, cascade)));

      } catch (OptimisticLockingException e) {
        this.exception = e;
      }
    }
  }

  protected void closeDownProcessEngine() {
    final ProcessEngine otherProcessEngine = ProcessEngines.getProcessEngine(PROCESS_ENGINE_NAME);
    if (otherProcessEngine != null) {

      ((ProcessEngineConfigurationImpl)otherProcessEngine.getProcessEngineConfiguration())
        .getCommandExecutorTxRequired()
        .execute((Command<Void>) commandContext -> {

          List<Job> jobs = otherProcessEngine.getManagementService().createJobQuery().list();
          if (jobs.size() > 0) {
            assertEquals(1, jobs.size());
            String jobId = jobs.get(0).getId();
            commandContext.getJobManager().deleteJob((JobEntity) jobs.get(0));
            commandContext.getHistoricJobLogManager().deleteHistoricJobLogByJobId(jobId);
          }

          return null;
        });

      otherProcessEngine.close();
      ProcessEngines.unregister(otherProcessEngine);
    }
  }

}