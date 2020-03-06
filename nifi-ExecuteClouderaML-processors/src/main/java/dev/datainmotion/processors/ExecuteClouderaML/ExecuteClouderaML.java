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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Tags({"cml", "Cloudera ML", "REST", "machine learning"})
@CapabilityDescription("Execute Cloudera Machine Learning CML")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="cml", description="CML attributes")})
@WritesAttributes({@WritesAttribute(attribute="post", description="post rest results")})
public class ExecuteClouderaML extends AbstractProcessor {

    /** output attribute name post.results will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_NAME = "cml.results";

    /** output attribute name post.header will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_HEADER = "cml.header";

    /** output attribute name post.status will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_STATUS = "cml.status";

    /** output attribute name post.statuscode will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_STATUS_CODE = "cml.statuscode";

    /** url   https://modelservice.cloudera.site/model */
    public static final PropertyDescriptor URL_NAME = new PropertyDescriptor.Builder().name("url")
            .description("URL Name like https://modelservice.cloudera.site/model ").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported( ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** header name */
    public static final PropertyDescriptor HEADER_NAME = new PropertyDescriptor.Builder().name("headername")
            .description("header name like Accept").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** header value */
    public static final PropertyDescriptor HEADER_VALUE = new PropertyDescriptor.Builder().name("headervalue")
            .description("Header Value like json").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** basic username steve */
    public static final PropertyDescriptor BASIC_USERNAME = new PropertyDescriptor.Builder().name("basicusername")
            .description("basic http username like susan").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** basic password toast */
    public static final PropertyDescriptor BASIC_PASSWORD = new PropertyDescriptor.Builder().name("basicpassword")
            .description("basic http password like iscool").required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();
    public static final String ACCESS_KEY = "accessKey";
    /** CML access key */
    public static final PropertyDescriptor CML_ACCESSKEY = new PropertyDescriptor.Builder().name( ACCESS_KEY )
            .description("cml model access key").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    public static final String CMLREQUEST = "cmlrequest";

    /** request json string */
    public static final PropertyDescriptor CML_REQUEST = new PropertyDescriptor.Builder().name( CMLREQUEST )
            .description("{\"sentence\":\"cloudera rocks\"}").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** Success of Relationship */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully determined image.").build();

    /** Failure of Relationship **/
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to determine image.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(URL_NAME);
        descriptors.add(CML_REQUEST);
        descriptors.add(CML_ACCESSKEY);
        descriptors.add(HEADER_NAME);
        descriptors.add(HEADER_VALUE);
        descriptors.add(BASIC_USERNAME);
        descriptors.add(BASIC_PASSWORD);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        return;
    }

    /**
     *
     * @param context
     * @param session
     * @throws ProcessException
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            flowFile = session.create();
        }
        try {
            flowFile.getAttributes();

            String urlName = flowFile.getAttribute("url");
            if (urlName == null) {
                urlName = context.getProperty("url").evaluateAttributeExpressions(flowFile).getValue();
            }
            if (urlName == null) {
                urlName = "https://cloudera.site/model";
            }

            String accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions(flowFile).getValue();
            if (accessKey == null) {
                accessKey = flowFile.getAttribute(ACCESS_KEY);
            }

            String cmlRequest =context.getProperty(CMLREQUEST).evaluateAttributeExpressions(flowFile).getValue();
            if (cmlRequest == null) {
                cmlRequest = flowFile.getAttribute(CMLREQUEST);
            }
            if (cmlRequest == null) {
                cmlRequest = "{}";
            }

            Map<String, String> attributes = flowFile.getAttributes();
            Map<String, String> attributesClean = new HashMap<>();

            try {

                System.out.println("url = " + urlName + " acces=" + accessKey + " cm=" + cmlRequest);

                HTTPPostResults results = HTTPPostUtility.postToCML( urlName, accessKey, cmlRequest );

                System.out.println("result=" + results.getHeader());
                
                if (results != null && results.getJsonResultBody() != null) {
                    try {
                        attributesClean.put(ATTRIBUTE_OUTPUT_NAME, results.getJsonResultBody());
                        attributesClean.put(ATTRIBUTE_OUTPUT_HEADER, results.getHeader());
                        attributesClean.put(ATTRIBUTE_OUTPUT_STATUS, results.getStatus());
                        attributesClean.put(ATTRIBUTE_OUTPUT_STATUS_CODE, String.valueOf(results.getStatusCode()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                else {
                    try {
                        attributesClean.put(ATTRIBUTE_OUTPUT_NAME, "Fail");
                        attributesClean.put(ATTRIBUTE_OUTPUT_HEADER, "Fail");
                        attributesClean.put(ATTRIBUTE_OUTPUT_STATUS, "FAIL");
                        attributesClean.put(ATTRIBUTE_OUTPUT_STATUS_CODE, "500");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            } catch (final Throwable t) {
                getLogger().error("Unable to process CML call " + t.getLocalizedMessage());
                getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
                throw t;
            }

        if (attributes.size() == 0) {
            session.transfer(flowFile, REL_FAILURE);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributesClean);
            session.transfer(flowFile, REL_SUCCESS);
        }
            session.commit();
        } catch (

                final Throwable t) {
            getLogger().error("Unable to process Cloudera machine learning " + t.getLocalizedMessage());
            throw new ProcessException(t);
        }
    }
}
