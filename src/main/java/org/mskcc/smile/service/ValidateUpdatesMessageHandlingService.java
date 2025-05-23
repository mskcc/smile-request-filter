package org.mskcc.smile.service;

import java.util.List;
import org.mskcc.cmo.messaging.Gateway;

public interface ValidateUpdatesMessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void requestUpdateFilterHandler(String requestJson) throws Exception;
    void sampleUpdateFilterHandler(List<Object> sampleJsonList) throws Exception;
    void shutdown() throws Exception;
}
