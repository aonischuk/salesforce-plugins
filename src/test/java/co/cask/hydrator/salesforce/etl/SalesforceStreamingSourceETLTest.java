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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.test.ProgramManager;
import co.cask.hydrator.salesforce.soap.SObjectBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sforce.soap.partner.sobject.SObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.List;

public class SalesforceStreamingSourceETLTest extends BaseSalesforceStreamingSourceETLTest {
  @Test
  public void testMyFunc() throws Exception {
    ProgramManager programManager = startPipeline(ImmutableMap.<String, String>builder()
      .put("pushTopicQuery", "SELECT id, naMe from Opportunity")
      .build());

    List<SObject> sObjects = new ImmutableList.Builder<SObject>()
      .add(new SObjectBuilder()
             .setType("Opportunity")
             .put("Name", "testValuesReturned12s62")
             .put("Amount", "25000")
             .put("StageName", "Proposal")
             .put("CloseDate", Date.from(Instant.now()))
             .put("TotalOpportunityQuantity", "25")
             .build())
      .build();

    addSObjects(sObjects);

    List<StructuredRecord> records = waitForRecords(programManager, sObjects.size());

    Assert.assertNotEquals(0, records.size());
  }
}
