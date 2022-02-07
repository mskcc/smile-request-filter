package org.mskcc.cmo.metadb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.util.Map;

public interface ValidRequestChecker {
    String getFilteredValidRequestJson(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
    Boolean isValidRequestMetadataJson(String requestJson)
            throws JsonMappingException, JsonProcessingException;
    Boolean isValidCmoSample(Map<String, String> sampleMap,
            boolean isCmoRequest, boolean hasRequestId) throws JsonMappingException, JsonProcessingException;
    Boolean isValidNonCmoSample(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException;
    Boolean isCmo(String json) throws JsonProcessingException;
    String getRequestId(String json) throws JsonProcessingException;
    Boolean hasRequestId(String json) throws JsonProcessingException;
}
