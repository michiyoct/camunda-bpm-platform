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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.OptimisticLockingException;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineBootstrapCommand;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricVariableInstance;
import org.camunda.bpm.engine.impl.BootstrapEngineCommand;
import org.camunda.bpm.engine.impl.ProcessEngineLogger;
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
import org.camunda.bpm.engine.impl.persistence.entity.HistoricVariableInstanceEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.test.RequiredDatabase;
import org.camunda.bpm.engine.management.JobDefinition;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.concurrency.ConcurrencyTestHelper;
import org.camunda.bpm.engine.test.concurrency.ConcurrentProcessEngineJobExecutorHistoryCleanupJobTest;
import org.camunda.bpm.engine.test.concurrency.ControllableThread;
import org.camunda.bpm.engine.test.concurrency.ControlledCommand;
import org.camunda.bpm.engine.test.jobexecutor.ControllableJobExecutor;
import org.camunda.bpm.engine.test.util.ProcessEngineBootstrapRule;
import org.camunda.bpm.engine.test.util.ProcessEngineTestRule;
import org.camunda.bpm.engine.test.util.ProvidedProcessEngineRule;
import org.camunda.bpm.engine.variable.Variables;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;

/**
 * Let's add CRDB-specific behavior tests here
 */
@RequiredDatabase(includes = DbSqlSessionFactory.CRDB)
public class CockroachSpecificBehaviorTest extends ConcurrencyTestHelper {

  protected static final Logger LOG = ProcessEngineLogger.TEST_LOGGER.getLogger();
  protected static final int COMMAND_RETRIES = 3;
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
  }

  @Ignore("WIP")
  @Test
  public void shouldRetryTxToBootstrapConcurrentProcessEngine() throws InterruptedException {
    // given
    ControllableProcessEngineBootstrap engineOne = new ControllableProcessEngineBootstrap("engine-one");
    ThreadControl engineOneThread = executeControllableCommand(engineOne);
    engineOneThread.waitForSync();

    ControllableProcessEngineBootstrap engineTwo = new ControllableProcessEngineBootstrap("engine-two");
    ThreadControl engineTwoThread = executeControllableCommand(engineTwo);
    engineTwoThread.reportInterrupts();
    engineTwoThread.waitForSync();

    engineOneThread.makeContinue(); // build process engine & execute bootstrap command

    // then
    engineTwoThread.makeContinue(); // build process engine & execute bootstrap command
    Thread.sleep(3000);

    engineOneThread.waitUntilDone(true);  // flush changes to db

    engineTwoThread.waitUntilDone(true); // attempt to flush, fail & retry

    assertThat(engineTwoThread.getException()).isNull();
    assertThat(engineOne.getTries()).isOne();
    assertThat(engineTwo.getTries()).isEqualTo(2);
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

  protected static class ControllableProcessEngineBootstrap extends ControllableCommand<Void> {

    protected ProcessEngineBootstrapCommand bootstrapCommand;
    protected int tries;
    protected String engineName;

    public ControllableProcessEngineBootstrap(String engineName) {
      this.tries = 0;
      this.engineName = engineName;
    }

    public Void execute(CommandContext commandContext) {

      bootstrapCommand = new ControllableBootstrapEngineCommand(this.monitor, engineName);

      ProcessEngineConfiguration processEngineConfiguration = ProcessEngineConfiguration
        .createProcessEngineConfigurationFromResource("camunda.cfg.xml");


      processEngineConfiguration.setProcessEngineBootstrapCommand(bootstrapCommand);
      processEngineConfiguration.setProcessEngineName(engineName);
      processEngineConfiguration.buildProcessEngine();

      return null;
    }

    @Override
    public boolean isRetryable() {
      return true;
    }

    public int getTries() {
      return ((ControllableBootstrapEngineCommand)bootstrapCommand).getTries();
    }
  }

  protected static class ControllableBootstrapEngineCommand extends ControllableCommand<Void> implements ProcessEngineBootstrapCommand {

    protected BootstrapEngineCommand bootstrapEngineCommand;
    protected int tries;
    protected String engineName;

    public ControllableBootstrapEngineCommand(ThreadControl monitor, String engineName) {
      super(monitor);
      this.bootstrapEngineCommand = new BootstrapEngineCommand();
      this.engineName = engineName;
      this.tries = 0;
    }

    @Override
    public Void execute(CommandContext commandContext) {

      if ("engine-two".equals(engineName)) {
        commandContext.getProcessEngineConfiguration().getHistoryService().findHistoryCleanupJobs();
      }

      monitor.sync();

      tries++;
      bootstrapEngineCommand.execute(commandContext);

      monitor.sync();

      return null;
    }

    @Override
    public boolean isRetryable() {
      return bootstrapEngineCommand.isRetryable();
    }

    public int getTries() {
      return tries;
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

  protected void makeEverLivingJobFail(final String jobId) {
    processEngineConfiguration.getCommandExecutorTxRequired().execute((Command<Void>) commandContext -> {

      JobEntity job = commandContext.getJobManager().findJobById(jobId);

      job.setExceptionStacktrace("foo");

      return null;
    });
  }

}