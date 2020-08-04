package org.camunda.bpm.engine.test.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.camunda.bpm.engine.CrdbTransactionRetryException;
import org.camunda.bpm.engine.RepositoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.externaltask.ExternalTask;
import org.camunda.bpm.engine.externaltask.LockedExternalTask;
import org.camunda.bpm.engine.impl.ProcessEngineImpl;
import org.camunda.bpm.engine.impl.cmd.DeployCmd;
import org.camunda.bpm.engine.impl.cmd.FetchExternalTasksCmd;
import org.camunda.bpm.engine.impl.db.sql.DbSqlSessionFactory;
import org.camunda.bpm.engine.impl.externaltask.TopicFetchInstruction;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.repository.DeploymentBuilderImpl;
import org.camunda.bpm.engine.impl.test.RequiredDatabase;
import org.camunda.bpm.engine.repository.DeploymentBuilder;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.concurrency.CompetingExternalTaskFetchingTest;
import org.camunda.bpm.engine.test.concurrency.CompetingJobAcquisitionTest;
import org.camunda.bpm.engine.test.concurrency.ConcurrencyTestHelper;
import org.camunda.bpm.engine.test.jobexecutor.ControllableJobExecutor;
import org.camunda.bpm.engine.test.jobexecutor.RecordingAcquireJobsRunnable.RecordedWaitEvent;
import org.camunda.bpm.engine.test.util.ProcessEngineBootstrapRule;
import org.camunda.bpm.engine.test.util.ProcessEngineTestRule;
import org.camunda.bpm.engine.test.util.ProvidedProcessEngineRule;
import org.camunda.bpm.model.bpmn.Bpmn;
import org.camunda.bpm.model.bpmn.BpmnModelInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

@Ignore
@RequiredDatabase(includes = DbSqlSessionFactory.CRDB)
public class CockroachDBRetriesTest extends ConcurrencyTestHelper {

  protected static final int COMMAND_RETRIES = 3;
  protected static final int DEFAULT_NUM_JOBS_TO_ACQUIRE = 3;
  protected final BpmnModelInstance PROCESS_WITH_USERTASK = Bpmn.createExecutableProcess("process")
      .startEvent()
        .userTask()
      .endEvent()
      .done();
  
  @ClassRule
  public static ProcessEngineBootstrapRule bootstrapRule = new ProcessEngineBootstrapRule(
      c -> c.setCommandRetries(COMMAND_RETRIES).setJobExecutor(new ControllableJobExecutor()));
  protected ProvidedProcessEngineRule engineRule = new ProvidedProcessEngineRule(bootstrapRule);
  protected ProcessEngineTestRule testRule = new ProcessEngineTestRule(engineRule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(engineRule).around(testRule);

  protected ControllableJobExecutor jobExecutor1;
  protected ControllableJobExecutor jobExecutor2;

  protected ThreadControl acquisitionThread1;
  protected ThreadControl acquisitionThread2;

  protected RepositoryService repositoryService;
  
  @Before
  public void setUp() throws Exception {
    processEngineConfiguration = engineRule.getProcessEngineConfiguration();
    repositoryService = engineRule.getRepositoryService();
    
    // two job executors with the default settings
    jobExecutor1 = (ControllableJobExecutor)
        processEngineConfiguration.getJobExecutor();
    jobExecutor1.setMaxJobsPerAcquisition(DEFAULT_NUM_JOBS_TO_ACQUIRE);
    acquisitionThread1 = jobExecutor1.getAcquisitionThreadControl();

    jobExecutor2 = new ControllableJobExecutor((ProcessEngineImpl) engineRule.getProcessEngine());
    jobExecutor2.setMaxJobsPerAcquisition(DEFAULT_NUM_JOBS_TO_ACQUIRE);
    acquisitionThread2 = jobExecutor2.getAcquisitionThreadControl();
  }

  @After
  public void tearDown() throws Exception {
    jobExecutor1.shutdown();
    jobExecutor2.shutdown();

    for(org.camunda.bpm.engine.repository.Deployment deployment : repositoryService.createDeploymentQuery().list()) {
      repositoryService.deleteDeployment(deployment.getId(), true);
    }
  }
  
  /**
   * See {@link CompetingJobAcquisitionTest#testCompetingJobAcquisitions} for the test
   * case without retries
   */
  @Test
  @Deployment(resources = "org/camunda/bpm/engine/test/jobexecutor/simpleAsyncProcess.bpmn20.xml")
  public void shouldRetryJobAcquisition() {

    // given
    int numJobs = DEFAULT_NUM_JOBS_TO_ACQUIRE + 1;
    for (int i = 0; i < numJobs; i++) {
      engineRule.getRuntimeService().startProcessInstanceByKey("simpleAsyncProcess").getId();
    }

    jobExecutor1.start();
    jobExecutor2.start();

    // both acquisition threads wait before acquiring something
    acquisitionThread1.waitForSync();
    acquisitionThread2.waitForSync();
    
    // both threads run the job acquisition query and should get overlapping results (3 out of 4 jobs)
    acquisitionThread1.makeContinueAndWaitForSync();
    acquisitionThread2.makeContinueAndWaitForSync();
    
    // thread1 flushes and commits first (success)
    acquisitionThread1.makeContinueAndWaitForSync();
    
    // when
    // acquisition fails => retry interceptor kicks in and retries command => waiting again before acquisition
    acquisitionThread2.makeContinueAndWaitForSync();

    // thread2 immediately acquires again and commits
    acquisitionThread2.makeContinueAndWaitForSync();
    acquisitionThread2.makeContinueAndWaitForSync();
    
    // then 
    // all jobs have been executed
    long currentJobs = engineRule.getManagementService().createJobQuery().active().count();
    assertThat(currentJobs).isEqualTo(0);
    
    // and thread2 has no reported failure
    assertThat(acquisitionThread2.getException()).isNull();
    
    List<RecordedWaitEvent> jobAcquisition2WaitEvents = jobExecutor2.getAcquireJobsRunnable().getWaitEvents();
    
    // and only one cycle of job acquisition was made (the wait event is from after the acquisition finished)
    assertThat(jobAcquisition2WaitEvents).hasSize(1); 
    Exception acquisitionException = jobAcquisition2WaitEvents.get(0).getAcquisitionException();
    
    // and the exception never bubbled up to the job executor (i.e. the retry was transparent)
    assertThat(acquisitionException).isNull(); 
  }
  
  
  /**
   * See {@link CompetingExternalTaskFetchingTest#testCompetingExternalTaskFetching()}
   * for the test case without retries.
   */
  @Test
  @Deployment(resources = "org/camunda/bpm/engine/test/concurrency/CompetingExternalTaskFetchingTest.testCompetingExternalTaskFetching.bpmn20.xml")
  public void shouldRetryExternalTaskFetchAndLock() {
    // given
    RuntimeService runtimeService = engineRule.getRuntimeService();
    
    int numTasksToFetch = 3;
    int numExternalTasks = numTasksToFetch + 1;
    
    for (int i = 0; i < numExternalTasks; i++) {
      runtimeService.startProcessInstanceByKey("oneExternalTaskProcess");
    }

    ThreadControl thread1 = executeControllableCommand(new ControlledFetchAndLockCommand(numTasksToFetch, "thread1", "externalTaskTopic"));
    ThreadControl thread2 = executeControllableCommand(new ControlledFetchAndLockCommand(numTasksToFetch, "thread2", "externalTaskTopic"));

    // thread1 and thread2 begin their transactions and fetch tasks
    thread1.waitForSync();
    thread2.waitForSync();
    thread1.makeContinueAndWaitForSync();
    thread2.makeContinueAndWaitForSync();
    
    // thread1 commits
    thread1.waitUntilDone();

    // when
    // thread2 flushes and fails => leads to retry
    thread2.waitUntilDone(true);
    
    // then
    List<ExternalTask> tasks = engineRule.getExternalTaskService().createExternalTaskQuery().list();
    List<ExternalTask> thread1Tasks = tasks.stream()
        .filter(t -> "thread1".equals(t.getWorkerId())).collect(Collectors.toList());
    List<ExternalTask> thread2Tasks = tasks.stream()
        .filter(t -> "thread2".equals(t.getWorkerId())).collect(Collectors.toList());
    
    assertThat(tasks).hasSize(numExternalTasks);
    assertThat(thread1Tasks).hasSize(numTasksToFetch);
    assertThat(thread2Tasks).hasSize(numExternalTasks - numTasksToFetch);
  }

  // TODO: disable pessimistic deployment lock. See CAM-12232
  @Test
  public void shouldRetryDeployCmd() throws InterruptedException {
    // given
    DeploymentBuilder deploymentOne = createDeploymentBuilder();
    DeploymentBuilder deploymentTwo = createDeploymentBuilder();

    // STEP 1: bring two threads to a point where they have
    // 1) started a new transaction
    // 2) are ready to deploy
    ThreadControl thread1 = executeControllableCommand(new ControllableDeployCommand(deploymentOne));
    thread1.waitForSync();

    ThreadControl thread2 = executeControllableCommand(new ControllableDeployCommand(deploymentTwo));
    thread2.waitForSync();

    // STEP 2: make Thread 1 proceed and wait until it has deployed but not yet committed
    // -> will still hold the exclusive lock
    thread1.makeContinue();
    thread1.waitForSync();

    // STEP 3: make Thread 2 continue
    // -> it will attempt to acquire the exclusive lock and block on the lock
    thread2.makeContinue();

    // wait for 2 seconds (Thread 2 is blocked on the lock)
    Thread.sleep(2000);

    // STEP 4: allow Thread 1 to terminate
    // -> Thread 1 will commit and release the lock
    thread1.waitUntilDone();

    // STEP 5: wait for Thread 2 to fail on flush and retry
    thread2.waitForSync();
    thread2.waitUntilDone(true);

    // ensure that although both transactions were run concurrently, the process definitions have different versions
    List<ProcessDefinition> processDefinitions = repositoryService
      .createProcessDefinitionQuery()
      .orderByProcessDefinitionVersion()
      .asc()
      .list();

    Assert.assertThat(processDefinitions.size(), is(2));
    Assert.assertThat(processDefinitions.get(0).getVersion(), is(1));
    Assert.assertThat(processDefinitions.get(1).getVersion(), is(2));
  }

  @Test
  public void shouldNotRetryCommandByDefault() {
    // given
    // a failing command
    ControllableFailingCommand failingCommand = new ControllableFailingCommand();
    ThreadControl failingCommandThread = executeControllableCommand(failingCommand);
    failingCommandThread.reportInterrupts();
    failingCommandThread.waitForSync();

    // when
    // the command is executed and fails
    failingCommandThread.waitUntilDone(true);

    // then
    // the exception is thrown to the called
    assertThat(failingCommandThread.getException()).isInstanceOf(CrdbTransactionRetryException.class);
    // and the command is only tried once
    assertThat(failingCommand.getTries()).isOne();
  }
  
  private static class ControlledFetchAndLockCommand extends ControllableCommand<List<LockedExternalTask>> {

    private FetchExternalTasksCmd wrappedCmd;
    
    public ControlledFetchAndLockCommand(int numTasks, String workerId, String topic) {
      Map<String, TopicFetchInstruction> instructions = new HashMap<String, TopicFetchInstruction>();

      TopicFetchInstruction instruction = new TopicFetchInstruction(topic, 10000L);
      instructions.put(topic, instruction);
      
      this.wrappedCmd = new FetchExternalTasksCmd(workerId, numTasks, instructions);
    }
    
    @Override
    public List<LockedExternalTask> execute(CommandContext commandContext) {
      monitor.sync();

      List<LockedExternalTask> tasks = wrappedCmd.execute(commandContext);
      
      monitor.sync();
        
      return tasks;
    }
    
    @Override
    public boolean isRetryable() {
      return wrappedCmd.isRetryable();
    }
    
  }

  protected static class ControllableDeployCommand extends ControllableCommand<Void> {

    protected final DeploymentBuilder deploymentBuilder;
    protected DeployCmd deployCmd;

    public ControllableDeployCommand(DeploymentBuilder deploymentBuilder) {
      this.deploymentBuilder = deploymentBuilder;
      this.deployCmd = new DeployCmd((DeploymentBuilderImpl) deploymentBuilder);
    }

    public Void execute(CommandContext commandContext) {
      monitor.sync();  // thread will block here until makeContinue() is called form main thread

      deployCmd.execute(commandContext);

      monitor.sync();  // thread will block here until waitUntilDone() is called form main thread

      return null;
    }

    @Override
    public boolean isRetryable() {
      return deployCmd.isRetryable();
    }
  }

  protected static class ControllableFailingCommand extends ControllableCommand<Void> {

    protected int tries = 0;

    @Override
    public Void execute(CommandContext commandContext) {

      monitor.sync();

      tries++;
      throw new CrdbTransactionRetryException("Does not retry");
    }

    public int getTries() {
      return tries;
    }

    @Override
    public boolean isRetryable() {
      return false;
    }
  }

  protected DeploymentBuilder createDeploymentBuilder() {
    return new DeploymentBuilderImpl(null)
      .name("some-deployment-name")
      .addModelInstance("foo.bpmn", PROCESS_WITH_USERTASK);
  }
}
