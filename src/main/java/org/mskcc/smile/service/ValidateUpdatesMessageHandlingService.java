package org.mskcc.smile.service;

import org.mskcc.cmo.messaging.Gateway;

public interface ValidateUpdatesMessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void requestUpdateFilterHandler(String requestJson) throws Exception;
    void sampleUpdateFilterHandler(String sampleJson) throws Exception;
    void shutdown() throws Exception;
}
