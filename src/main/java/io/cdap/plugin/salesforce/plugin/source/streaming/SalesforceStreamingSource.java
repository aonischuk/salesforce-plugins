/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package io.cdap.plugin.salesforce.plugin.source.streaming;

import com.google.common.base.Strings;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;

/**
 * Returns records in realtime created by Salesforce. To achieve this Salesforce Streaming API is used.
 * We use cometd server to subscribe to Salesforce Push Topics and receive realtime updates from there
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SalesforceStreamingSource.NAME)
@Description(SalesforceStreamingSource.DESCRIPTION)
public class SalesforceStreamingSource extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceStreamingSource.class);

  static final String NAME = "Salesforce";
  static final String DESCRIPTION = "Streams data updates from Salesforce using Salesforce Streaming API";
  private SalesforceStreamingSourceConfig config;
  private Schema schema;

  public SalesforceStreamingSource(SalesforceStreamingSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // Verify that reference name meets dataset id constraints
    IdUtils.validateId(config.referenceName);
    pipelineConfigurer.createDataset(config.referenceName, Constants.EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);

    try {
      config.validate(); // validate when macros are not substituted
      config.ensurePushTopicExistAndWithCorrectFields(); // run when macros are not substituted

      String query = config.getQuery();

      if (!Strings.isNullOrEmpty(query)
        && !config.containsMacro(SalesforceStreamingSourceConfig.PROPERTY_PUSH_TOPIC_QUERY)
        && !config.containsMacro(SalesforceStreamingSourceConfig.PROPERTY_SOBJECT_NAME)) {

        Schema schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(),
                                                       SObjectDescriptor.fromQuery(query));
        pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      }
    } catch (ConnectionException e) {
      throw new InvalidStageException("There was issue communicating with Salesforce", e);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws ConnectionException {
    config.validate(); // validate when macros are substituted
    config.ensurePushTopicExistAndWithCorrectFields(); // run when macros are substituted

    this.schema = streamingContext.getOutputSchema();

    if (this.schema == null) { // if was not set in configurePipeline due to fields containing macro
      this.schema = SalesforceSchemaUtil.getSchema(config.getAuthenticatorCredentials(),
                                                   SObjectDescriptor.fromQuery(config.getQuery()));
    }
    LOG.debug("Schema is {}", schema);

    SalesforceStreamer streamer = new SalesforceStreamer(schema, config);

    return streamer.getReceiverStream(streamingContext);
  }

  @Path("outputSchema")
  public Schema outputSchema(SalesforceStreamingSourceConfig config) throws Exception {
    AuthenticatorCredentials authenticatorCredentials = config.getAuthenticatorCredentials();
    PartnerConnection partnerConnection = new PartnerConnection(
      Authenticator.createConnectorConfig(authenticatorCredentials));
    SObject pushTopic =
      SalesforceStreamingSourceConfig.fetchPushTopicByName(partnerConnection, config.getPushTopicName());

    String query;
    if (pushTopic == null) {
      // PushTopic is not yet created. This will be done during pipeline run
      query = config.getQuery();
    } else {
      // In case user does not set any query in the corresponding field we need to get it from pushTopic on server
      query = (String) pushTopic.getField("Query");
    }

    return SalesforceSchemaUtil.getSchema(authenticatorCredentials,
                                   SObjectDescriptor.fromQuery(query));
  }
}
