package org.mskcc.smile;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.smile.service.impl.PromotedRequestMsgHandlingServiceImpl;
import org.mskcc.smile.service.impl.ValidRequestCheckerImpl;
import org.mskcc.smile.service.impl.ValidateUpdatesMsgHandlingServiceImpl;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

/**
 *
 * @author laptop
 */
@SpringBootApplication(scanBasePackages = {"org.mskcc.smile.service", "org.mskcc.smile.commons.*"})
public class SmileRequestFilterTestApp {
    @Bean
    public ValidRequestCheckerImpl validRequestCheckerImpl() {
        return new ValidRequestCheckerImpl();
    }

    @MockBean
    public Gateway messagingGateway;

    @MockBean
    public PromotedRequestMsgHandlingServiceImpl promotedRequestMsgHandlingService;

    @MockBean
    public ValidateUpdatesMsgHandlingServiceImpl validateUpdatesMsgHandlingService;
}
