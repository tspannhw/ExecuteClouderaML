/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.datainmotion.processors.ExecuteClouderaML;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class ExecuteClouderaMLTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner( ExecuteClouderaML.class);
    }

    @Test
    public void testProcessor() {
    	testRunner.setProperty("url", "https://modelservice.X.cloudera.site/model");
    	testRunner.setProperty("accessKey", "X");
    	testRunner.setProperty("cmlrequest", "cloudera rocks");
        // testRunner.enqueue();

        // Must add valid url for integration test
       // runAndAssertHappy();
    }

    /**
     *
     */
    private void runAndAssertHappy() {
        testRunner.setValidateExpressionUsage(false);
        testRunner.run();
        testRunner.assertValid();
        testRunner.assertAllFlowFilesTransferred(ExecuteClouderaML.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(ExecuteClouderaML.REL_SUCCESS);

        if ( successFiles == null || successFiles.size() <=0) {
            System.out.println("fail");
        }
        for (MockFlowFile mockFile : successFiles) {
            Map<String, String> attributes =  mockFile.getAttributes();

            for (String attribute : attributes.keySet()) {
                System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
            }

            assertNotNull(mockFile.getAttribute(ExecuteClouderaML.ATTRIBUTE_OUTPUT_HEADER));
            assertNotNull(mockFile.getAttribute(ExecuteClouderaML.ATTRIBUTE_OUTPUT_STATUS_CODE));
            assertNotNull(mockFile.getAttribute(ExecuteClouderaML.ATTRIBUTE_OUTPUT_STATUS));
            assertNotNull(mockFile.getAttribute(ExecuteClouderaML.ATTRIBUTE_OUTPUT_NAME));
        }

    }

}
