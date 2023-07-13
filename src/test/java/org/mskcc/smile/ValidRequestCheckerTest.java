package org.mskcc.smile;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.smile.config.MockDataConfig;
import org.mskcc.smile.model.MockJsonTestData;
import org.mskcc.smile.service.ValidRequestChecker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(classes = MockDataConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ComponentScan("org.mskcc.smile.service")
public class ValidRequestCheckerTest {
    private ObjectMapper mapper = new ObjectMapper();

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
                .get("mockIncomingRequest1JsonDataWith2T2N");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            Assert.assertTrue((Boolean) sampleStatus.get("validationStatus"));
        }
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
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            Assert.assertTrue((Boolean) sampleStatus.get("validationStatus"));
        }
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
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertFalse((Boolean) requestStatus.get("validationStatus"));

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            Assert.assertFalse((Boolean) sampleStatus.get("validationStatus"));
        }
    }

    /**
     * Test for handling null or empty baitSet in sampleManifest inrequestJson
     */
    @Test
    public void testNullBaitSetInSampleManifestInRequestJson() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1eNullStringBaitSet");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertFalse((Boolean) requestStatus.get("validationStatus"));

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            Assert.assertFalse((Boolean) sampleStatus.get("validationStatus"));
        }
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
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertFalse((Boolean) requestStatus.get("validationStatus"));

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            Assert.assertFalse((Boolean) sampleStatus.get("validationStatus"));
        }
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
                "{\"smileRequestId\": \"smileRequestIdValue\",  \"igoProjectId\": \"MOCKREQUEST1\"}";
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

    @Test
    public void testValidPromotedRequest() throws Exception {
        String requestJson = "{\"requestId\":\"1456_T\",\"projectId\":\"1456\",\"isCmoRequest\":"
                + "true,\"samples\":[{\"igoId\":\"1456_T_1\",\"cmoPatientId\":\"C-8484\"}]}";
        Map<String, Object> promotedRequestJsonMap =
                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
        Map<String, Object> requestStatus =
                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));
    }

    @Test
    public void testValidPromotedRequestFailedSample() throws Exception {
        String requestJson = "{\"requestId\":\"1456_T\",\"projectId\":\"1456\",\"isCmoRequest\":"
                + "true,\"samples\":[{\"notTheRightIdentifier\":\"1456_T_1\",\"cmoPatientId\":\"C-8484\"}]}";
        Map<String, Object> promotedRequestJsonMap =
                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
        Map<String, Object> requestStatus =
                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
        Assert.assertFalse((Boolean) requestStatus.get("validationStatus"));
    }

    @Test
    public void testValidPromotedRequestUniversalSchema() throws Exception {
        String requestJson = "{\"igoRequestId\":\"1456_T\",\"igoProjectId\":\"1456\",\"isCmoRequest\":"
                + "true,\"samples\":[{\"primaryId\":\"1456_T_1\",\"cmoPatientId\":\"C-8484\"}]}";
        Map<String, Object> promotedRequestJsonMap =
                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
        Map<String, Object> requestStatus =
                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));
    }

    @Test
    public void testValidPromotedRequestMissingProjectId() throws Exception {
        String requestJson = "{\"igoRequestId\":\"1456_T\",\"isCmoRequest\":"
                + "true,\"samples\":[{\"primaryId\":\"1456_T_1\",\"cmoPatientId\":\"C-8484\"}]}";
        Map<String, Object> promotedRequestJsonMap =
                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
        Map<String, Object> requestStatus =
                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));
    }

    @Test
    public void testValidPromotedRequestMissingRequestId() throws Exception {
        String requestJson = "{\"projectId\":\"1456\",\"isCmoRequest\":"
                + "true,\"samples\":[{\"igoId\":\"1456_T_1\",\"cmoPatientId\":\"C-8484\"}]}";
        Map<String, Object> promotedRequestJsonMap =
                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
        Map<String, Object> requestStatus =
                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
        Assert.assertFalse((Boolean) requestStatus.get("validationStatus"));
    }

    @Test
    public void testValidPromotedSampleWithNormPatientId() throws Exception {
        String requestJson = "{\"requestId\":\"1456_T\",\"projectId\":\"1456\",\"isCmoRequest\":"
                + "true,\"samples\":[{\"igoId\":\"1456_T_1\",\"cmoSampleIdFields\":"
                + "{\"normalizedPatientId\":\"C-8484\"}}]}";
        Map<String, Object> promotedRequestJsonMap =
                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
        Map<String, Object> requestStatus =
                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));
    }

    /**
     * Test for handling request with 2 samples missing fastqs. This would still
     * pass the sanity check but the request will be logged with warnings.
     *
     */
    @Test
    public void testRequestJsonWithSamplesMissingFastqs() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1SamplesMissingFastQs");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNotNull(modifiedRequestJson);
    }

    /**
     * Test for handling request with all samples missing fastqs. This request
     * will fail the sanity check.
     *
     */
    @Test
    public void testRequestJsonWithAllSamplesMissingFastqs() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest1AllSamplesMissingFastQs");
        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertFalse((Boolean) requestStatus.get("validationStatus"));

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            Assert.assertFalse((Boolean) sampleStatus.get("validationStatus"));
        }
    }

    /**
     * Tests that mocked request has no valid samples and that the samples are instead in the request-level
     * validation report.
     * @throws Exception
     */
    @Test
    public void testRequestSamplesMissingPatientIds() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap.get("mockRequest7SamplesMissingPids");
        String modifiedRequestJson =
                validRequestChecker.getFilteredValidRequestJson(requestJson.getJsonString());
        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        // assert sample list is empty in the modified request json
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        Assert.assertTrue(sampleList.length == 1);

        // assert request validation status is true since there's at least one valid sample
        Map<String, Object> requestStatus = mapper.convertValue(requestJsonMap.get("status"), Map.class);
        Assert.assertTrue((Boolean) requestStatus.get("validationStatus"));

        // assert request validation report has 'samples' and length is 4
        Map<String, Object> validationReport =
                mapper.convertValue(requestStatus.get("validationReport"), Map.class);
        Object[] failedSamplesList = mapper.convertValue(validationReport.get("samples"),
                Object[].class);
        Assert.assertTrue(failedSamplesList.length == 3);
    }

    /**
     * Test for handling request with 2 out of 4 samples having igoComplete set to false.
     *
     */
    @Test
    public void testRequestJsonWithSamplesHavingFalseIgoComplete() throws Exception {
        MockJsonTestData requestJson = mockedRequestJsonDataMap
                .get("mockRequest8FalseIgoComplete");

        String modifiedRequestJson = validRequestChecker
                .getFilteredValidRequestJson(requestJson.getJsonString());
        Assert.assertNotNull(modifiedRequestJson);

        Map<String, Object> requestJsonMap = mapper.readValue(modifiedRequestJson, Map.class);
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = mapper.convertValue(sampleMap.get("status"), Map.class);
            if (sampleMap.get("igoComplete") == Boolean.FALSE.toString()) {
                Assert.assertFalse((Boolean) sampleStatus.get("validationStatus"));
            }
        }
    }
}
