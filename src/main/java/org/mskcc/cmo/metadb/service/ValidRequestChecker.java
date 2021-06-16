package org.mskcc.cmo.metadb.service;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public interface ValidRequestChecker {
    public String checkIfValidRequest(String requestJson) throws JsonMappingException, JsonProcessingException, IOException;
}
