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
    public static final String ATTRIBUTE_OUTPUT_NAME = "post.results";

    /** output attribute name post.header will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_HEADER = "post.header";

    /** output attribute name post.status will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_STATUS = "post.status";

    /** output attribute name post.statuscode will contain JSON **/
    public static final String ATTRIBUTE_OUTPUT_STATUS_CODE = "post.statuscode";

    /** url http://127.0.0.1:9999/squeezenet/predict  */
    public static final PropertyDescriptor URL_NAME = new PropertyDescriptor.Builder().name("url")
            .description("URL Name like http://127.0.0.1:9999/squeezenet/predict").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported( ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** fieldname "data" */
    public static final PropertyDescriptor FIELD_NAME = new PropertyDescriptor.Builder().name("fieldname")
            .description("Field Name like data").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** imagename imageName  "/TimKafka.jpg" */
    public static final PropertyDescriptor IMAGE_NAME = new PropertyDescriptor.Builder().name("imagename")
            .description("Image Name like TimLovesNiFi.jpg").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    /** imageType "image/jpeg" */
    public static final PropertyDescriptor IMAGE_TYPE = new PropertyDescriptor.Builder().name("imagetype")
            .description("Image Type like image/jpeg").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

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
        descriptors.add(FIELD_NAME);
        descriptors.add(IMAGE_NAME);
        descriptors.add(IMAGE_TYPE);
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
            return;
        }
        try {
            flowFile.getAttributes();

            /** test
             *
             * curl -H "Content-Type: application/json" -X POST https://modelservice.ml-1c323959-8ec.cdp-cldr.ylcu-atmi.cloudera.site/model -d
             * '{"accessKey":"mziy46a87ob7ix6aflv9lteopxbk9mlt",
             * "request":{"sentence":"Cloudera is the best ever"}}'
             *
             */
            String urlName = flowFile.getAttribute("url");
            if (urlName == null) {
                urlName = context.getProperty("url").evaluateAttributeExpressions(flowFile).getValue();
            }
            if (urlName == null) {
                urlName = "http://localhost:8080/nifi";
            }

            String fieldname = flowFile.getAttribute("fieldname");
            if (fieldname == null) {
                fieldname = context.getProperty("fieldname").evaluateAttributeExpressions(flowFile).getValue();
            }
            if (fieldname == null) {
                fieldname = "data";
            }

            String imagename = flowFile.getAttribute("imagename");
            if (imagename == null) {
                imagename = context.getProperty("imagename").evaluateAttributeExpressions(flowFile).getValue();
            }
            if (imagename == null) {
                imagename = "test.jpg";
            }

            String imagetype = flowFile.getAttribute("imagetype");
            if (imagetype == null) {
                imagetype = context.getProperty("imagetype").evaluateAttributeExpressions(flowFile).getValue();
            }
            if (imagetype == null) {
                imagetype = "images/jpeg";
            }

            String headername = flowFile.getAttribute("headername");
            if (headername == null) {
                headername = context.getProperty("headername").evaluateAttributeExpressions(flowFile).getValue();
            }

            String headervalue = flowFile.getAttribute("headervalue");
            if (headervalue == null) {
                headervalue = context.getProperty("headervalue").evaluateAttributeExpressions(flowFile).getValue();
            }

            String basicusername = flowFile.getAttribute("basicusername");
            if (basicusername == null) {
                basicusername = context.getProperty("basicusername").evaluateAttributeExpressions(flowFile).getValue();
            }

            String basicpassword = flowFile.getAttribute("basicpassword");
            if (basicpassword == null) {
                basicpassword = context.getProperty("basicpassword").evaluateAttributeExpressions(flowFile).getValue();
            }

            final String url = urlName;
            final String field = fieldname;
            final String image = imagename;
            final String imgtype = imagetype;
            final String headerName = headername;
            final String headerValue = headervalue;
            final String basicUserName = basicusername;
            final String basicPassword = basicpassword;

            try {
                final HashMap<String, String> attributes = new HashMap<String, String>();

                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(InputStream input) throws IOException {
                        if ( input == null ) {
                            return;
                        }
                        HTTPPostResults results = null;
//HTTPPostUtility.postImage(url, field, image, imgtype, input, headerName, headerValue, basicUserName, basicPassword);

                        if (results != null && results.getJsonResultBody() != null) {
                            try {
                                attributes.put(ATTRIBUTE_OUTPUT_NAME, results.getJsonResultBody());
                                attributes.put(ATTRIBUTE_OUTPUT_HEADER, results.getHeader());
                                attributes.put(ATTRIBUTE_OUTPUT_STATUS, results.getStatus());
                                attributes.put(ATTRIBUTE_OUTPUT_STATUS_CODE, String.valueOf(results.getStatusCode()));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        else {
                            try {
                                attributes.put(ATTRIBUTE_OUTPUT_NAME, "Fail");
                                attributes.put(ATTRIBUTE_OUTPUT_HEADER, "Fail");
                                attributes.put(ATTRIBUTE_OUTPUT_STATUS, "FAIL");
                                attributes.put(ATTRIBUTE_OUTPUT_STATUS_CODE, "500");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                if (attributes.size() == 0) {
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.transfer(flowFile, REL_SUCCESS);
                }
            } catch (Exception e) {
                throw new ProcessException(e);
            }

            session.commit();
        } catch (

                final Throwable t) {
            getLogger().error("Unable to process Post Image Processor file " + t.getLocalizedMessage());
            throw new ProcessException(t);
        }
    }
}
