package org.mskcc.cmo.metadb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;

public interface ValidRequestChecker {
    public String getFilteredValidRequestJson(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException;
}
