package org.mskcc.cmo.metadb.service;

import org.mskcc.cmo.messaging.Gateway;

public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void requestFilterHandler(String requestJson) throws Exception;
    void shutdown() throws Exception;
}
