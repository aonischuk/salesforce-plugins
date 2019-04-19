/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.salesforce.etl;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.datastreams.DataStreamsApp;
import co.cask.cdap.datastreams.DataStreamsSparkLauncher;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.DataStreamsConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.etl.spark.Compat;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.hydrator.salesforce.SalesforceConnectionUtil;
import co.cask.hydrator.salesforce.authenticator.AuthenticatorCredentials;
import co.cask.hydrator.salesforce.plugin.source.streaming.SalesforceStreamingSource;
import co.cask.hydrator.salesforce.plugin.source.streaming.SalesforceStreamingSourceConfig;
import com.google.common.collect.ImmutableMap;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BaseSalesforceStreamingSourceETLTest extends HydratorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSalesforceStreamingSourceETLTest.class);
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-streams", "1.0.0");
  private static final int SALESFORCE_RECEIVER_START_TIMEOUT_SECONDS = 60;
  private static final long SALESFORCE_RECEIVER_START_POLLING_INTERVAL_MS = 100;
  private static final String SALESFORCE_RECEIVER_NAME_PREFIX = "salesforce_streaming_api_listener";
  private static final int WAIT_FOR_RECORDS_TIMEOUT_SECONDS = 60;
  private static final long WAIT_FOR_RECORDS_POLLING_INTERVAL_MS = 100;

  protected static PartnerConnection partnerConnection;

  @ClassRule
  public static final TestConfiguration CONFIG =
    new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false,
                          Constants.AppFabric.SPARK_COMPAT, Compat.SPARK_COMPAT);

  /*
  private static final String CLIENT_ID = System.getProperty("salesforce.test.clientId");
  private static final String CLIENT_SECRET = System.getProperty("salesforce.test.clientSecret");
  private static final String USERNAME = System.getProperty("salesforce.test.username");
  private static final String PASSWORD = System.getProperty("salesforce.test.password");
  */
  private static final String CLIENT_ID =
    "3MVG9T46ZAw5GTfW5yPRPWajojsj13Mqxh0hq0r7J1S9FQDzTEvKS41JMbhSu9E_X6VxEg4tUt.9OrXPy14DF";
  private static final String CLIENT_SECRET = "4C9C44A6020167C56E745B5BA9DB962114E150500BABE0B36715E9B00D08D495";
  private static final String USERNAME = "empowers33@gmail.com";
  private static final String PASSWORD = "cdappassword1";

  private static final String LOGIN_URL = System.getProperty("salesforce.test.loginUrl",
                                                             "https://login.salesforce.com/services/oauth2/token");

  @Rule
  public TestName name = new TestName();

  private List<String> createdObjectsIds = new ArrayList<>();

  @BeforeClass
  public static void setupTest() throws Exception {
    LOG.info("Setting up application");

    setupStreamingArtifacts(APP_ARTIFACT_ID, DataStreamsApp.class);

    LOG.info("Setting up plugins");

    addPluginArtifact(NamespaceId.DEFAULT.artifact("salesforce-plugins", "1.0.0"),
                      APP_ARTIFACT_ID,
                      SalesforceStreamingSource.class,
                      AuthenticatorCredentials.class
    );

    partnerConnection = SalesforceConnectionUtil.getPartnerConnection(SalesforceConnectionUtil.
      getAuthenticatorCredentials(
      USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET, LOGIN_URL));
  }

  @After
  public void cleanUp() throws ConnectionException {
    clearSObjects();
  }

  protected ProgramManager startPipeline(Map<String, String> properties) throws Exception {
    String pushTopicName = name.getMethodName();

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .put("referenceName", "SalesforceBulk-input")
      .put("clientId", CLIENT_ID)
      .put("clientSecret", CLIENT_SECRET)
      .put("username", USERNAME)
      .put("password", PASSWORD)
      .put("loginUrl", LOGIN_URL)
      .put("errorHandling", "Stop on error")
      .put("pushTopicName", pushTopicName)
      .put("pushTopicNotifyCreate", "Enabled")
      .put("pushTopicNotifyUpdate", "Enabled")
      .put("pushTopicNotifyDelete", "Enabled")
      .put("pushTopicNotifyForFields", "Referenced")
      .putAll(properties)
      .build();

    ETLPlugin sourceConfig = new ETLPlugin("SalesforceStreaming", StreamingSource.PLUGIN_TYPE, sourceProps);
    ETLPlugin sinkConfig = MockSink.getPlugin(getOutputDatasetName());

    ProgramManager programManager = deployETL(sourceConfig, sinkConfig, "SalesforceStreaming_" + name.getMethodName());
    programManager.startAndWaitForRun(ProgramRunStatus.RUNNING, 30, TimeUnit.SECONDS);

    // Wait for Spark to start up the job by checking if receiver thread exists
    try {
      Awaitility.await()
        .atMost(SALESFORCE_RECEIVER_START_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(SALESFORCE_RECEIVER_START_POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .until(this::hasReceiverThreadStarted);
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException(String.format("Salesforce receiver thread failed to start in %d seconds",
                                                    SALESFORCE_RECEIVER_START_TIMEOUT_SECONDS), e);
    }

    SObject pushTopic = SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, pushTopicName);
    createdObjectsIds.add((String) pushTopic.getField("Id"));

    return programManager;
  }

  protected String getOutputDatasetName() {
    return "output-realtimesourcetest_" + name.getMethodName();
  }

  protected List<StructuredRecord> waitForRecords(ProgramManager programManager,
                                                  int exceptedNumberOfRecords) throws Exception {
    DataSetManager<Table> outputManager = getDataset(getOutputDatasetName());

    try {
      Awaitility.await()
        .atMost(WAIT_FOR_RECORDS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_FOR_RECORDS_POLLING_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .until(() -> MockSink.readOutput(outputManager).size() == exceptedNumberOfRecords);
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException(String.format("Timeout occured while waiting for %d records", exceptedNumberOfRecords), e);
    }

    programManager.stop();
    programManager.waitForStopped(10, TimeUnit.SECONDS);
    programManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);

    return MockSink.readOutput(outputManager);
  }

  /**
   * Adds sObjects to Salesforce. Checks the result response for errors.
   * If save flag is true, saves the objects so that they can be deleted after method is run.
   *
   * @param sObjects list of sobjects to create
   */
  protected void addSObjects(List<SObject> sObjects) {
    try {
      SaveResult[] results = partnerConnection.create(sObjects.toArray(new SObject[0]));
      createdObjectsIds.addAll(Arrays.stream(results)
        .map(SaveResult::getId).collect(Collectors.toList()));

      for (SaveResult saveResult : results) {
        if (!saveResult.getSuccess()) {
          String allErrors = Stream.of(saveResult.getErrors())
            .map(Error::getMessage)
            .collect(Collectors.joining("\n"));

          throw new RuntimeException(allErrors);
        }
      }

    } catch (ConnectionException e) {
      throw new RuntimeException("There was issue communicating with Salesforce", e);
    }
  }

  private SparkManager deployETL(ETLPlugin sourcePlugin, ETLPlugin sinkPlugin, String appName) throws Exception {
    ETLStage source = new ETLStage("source", sourcePlugin);
    ETLStage sink = new ETLStage("sink", sinkPlugin);
    DataStreamsConfig etlConfig = DataStreamsConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .setBatchInterval("1s")
      .build();

    AppRequest<DataStreamsConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    return applicationManager.getSparkManager(DataStreamsSparkLauncher.NAME);
  }

  private void clearSObjects() throws ConnectionException {
    if (createdObjectsIds.isEmpty()) {
      return;
    }

    partnerConnection.delete(createdObjectsIds.toArray(new String[0]));
    createdObjectsIds.clear();
  }

  private boolean hasReceiverThreadStarted() {
    Set<String> resultSet = Thread.getAllStackTraces().keySet()
      .stream()
      .map(Thread::getName)
      .filter(name -> name.startsWith(SALESFORCE_RECEIVER_NAME_PREFIX))
      .collect(Collectors.toSet());

    return !resultSet.isEmpty();
  }
}
