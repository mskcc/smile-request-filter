package org.mskcc.smile.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.util.Map;

public interface ValidRequestChecker {
    String getFilteredValidRequestJson(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
    Map<String, Object> generateRequestStatusValidationMap(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
    Map<String, Object> generateCmoSampleValidationMap(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException;
    Map<String, Object> generateNonCmoSampleValidationMap(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException;
    Boolean isCmo(String json) throws JsonProcessingException;
    String getRequestId(String json) throws JsonProcessingException;
    Boolean hasRequestId(String json) throws JsonProcessingException;
    Map<String, Object> generatePromotedRequestValidationMap(String requestJson) throws JsonMappingException,
            JsonProcessingException, IOException;
    Map<String, Object> generatePromotedSampleValidationMap(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException;
}
