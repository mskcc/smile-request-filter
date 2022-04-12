package org.mskcc.smile.service;

import org.mskcc.cmo.messaging.Gateway;

public interface RequestFilterMessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void requestFilterHandler(String requestJson) throws Exception;
    void shutdown() throws Exception;
}
