package org.mskcc.cmo.metadb;

import java.util.Map;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cmo.metadb.config.MockDataConfig;
import org.mskcc.cmo.metadb.model.MockJsonTestData;
import org.mskcc.cmo.metadb.service.ValidRequestChecker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(classes = MockDataConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan("org.mskcc.cmo.metadb.service")
public class ValidRequestCheckerTest {

    @Autowired
    ValidRequestChecker validRequestChecker;

    @Autowired
    private Map<String, MockJsonTestData> mockedRequestJsonDataMap;

    /**
     * Tests to ensure the mocked request json data map is not null
     */
    @Test
    public void testMockedRequestJsonDataLoading() {
        Assert.assertNotNull(mockedRequestJsonDataMap);
    }

    /**
     * Test for handling of fields that are a mix of null or empty strings in the requestJson
     * The filter is expected to fail
     */
    @Test
    public void testValidRequestJson() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1JsonDataWith2T2N");
        String modifiedRequestJson = requestJson.getJsonString();
        Assert.assertNotNull(modifiedRequestJson);
    }

    /**
     * Test for handling all null or empty fields in requestJson
     */
    @Test
    public void testNullJsonFieldHandlingInRequestJson() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest4JsonNullOrEmptyValues");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNull(modifiedRequestJson);
    }

    /**
     * Test for handling null or empty sampleMetadata in requestJson
     */
    @Test
    public void testSingleNullSampleManifestInRequestJson() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest2aEmptySampleManifestValues");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNull(modifiedRequestJson);
    }


    /**
     * Test for handling request with no valid samples
     */
    @Test
    public void testRequestJsonWithNoValidSamples() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1cJsonDataWithMAS");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNull(modifiedRequestJson);
    }

    /**
     * Test for handling request with 2 valid samples and 2 invalid samples
     *
     */
    @Test
    public void testRequestJsonWithTwoInvalidSamples() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1dJsonDataWithMFS");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNotSame(modifiedRequestJson, requestJson.getJsonString());
    }

    /**
     * Test for handling request with available fields to determine sample type abbreviation
     *
     */
    @Test
    public void testRequestJsonWithMissingFieldsUsedForSTA() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1aJsonDataWithSTA");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNotNull(modifiedRequestJson);
    }

    /**
     * Test for handling request with available fields to determine nucleic acid abbreviation
     *
     */
    @Test
    public void testRequestJsonWithMissingFieldsUsedForNAA() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1bJsonDataWithNAA");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNotNull(modifiedRequestJson);
    }

    @Test
    public void testGetRequestIdMissingRequestId() throws Exception {
        String requestJson =
                "{\"metaDbRequestId\": \"metaDbRequestIdValue\",  \"igoProjectId\": \"MOCKREQUEST1\"}";
        String requestId = validRequestChecker.getRequestId(requestJson);
        Assert.assertNull(requestId);
    }

    @Test
    public void testGetRequestIdWithNullJson() throws Exception {
        String requestId = validRequestChecker.getRequestId(null);
        Assert.assertNull(requestId);
    }

    @Test
    public void testGetRequestIdWithEmptyJsonString() throws Exception {
        String requestId = validRequestChecker.getRequestId("");
        Assert.assertNull(requestId);
    }
}
