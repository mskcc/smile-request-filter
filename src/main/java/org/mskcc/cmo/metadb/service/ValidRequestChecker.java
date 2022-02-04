package org.mskcc.cmo.metadb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.util.Map;

public interface ValidRequestChecker {
    public String getFilteredValidRequestJson(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
    public Boolean isValidRequestMetadataJson(String requestJson)
            throws JsonMappingException, JsonProcessingException;
    public boolean isValidCmoSample(Map<String, String> sampleMap,
            boolean isCmoRequest, boolean hasRequestId) throws JsonMappingException, JsonProcessingException;
    public boolean isValidNonCmoSample(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException;
}
