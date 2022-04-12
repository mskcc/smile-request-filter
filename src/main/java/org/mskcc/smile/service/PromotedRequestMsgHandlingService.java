package org.mskcc.smile.service;

import org.mskcc.cmo.messaging.Gateway;

/**
 *
 * @author ochoaa
 */
public interface PromotedRequestMsgHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void promotedRequestHandler(String requestJson) throws Exception;
    void shutdown() throws Exception;
}
