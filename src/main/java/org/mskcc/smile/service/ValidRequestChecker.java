package org.mskcc.smile.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.opentelemetry.api.trace.Span;
import java.io.IOException;
import java.util.Map;

public interface ValidRequestChecker {
    String getFilteredValidRequestJson(Span parentSpan, String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
    Boolean hasValidRequestLevelMetadata(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
    Boolean isValidCmoSample(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException;
    Boolean isValidNonCmoSample(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException;
    Boolean isCmo(String json) throws JsonProcessingException;
    String getRequestId(String json) throws JsonProcessingException;
    Boolean hasRequestId(String json) throws JsonProcessingException;
    Boolean isValidPromotedRequest(String requestJson) throws JsonMappingException,
            JsonProcessingException, IOException;
}
