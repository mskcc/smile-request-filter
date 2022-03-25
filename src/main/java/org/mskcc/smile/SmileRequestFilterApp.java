package org.mskcc.smile;

import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.smile.service.MessageHandlingService;
import org.mskcc.smile.service.ValidateUpdatesMessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"org.mskcc.cmo.messaging",
        "org.mskcc.smile.commons.*", "org.mskcc.smile.*"})
public class SmileRequestFilterApp implements CommandLineRunner {
    private static final Log LOG = LogFactory.getLog(SmileRequestFilterApp.class);

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    private MessageHandlingService messageHandlingService;

    @Autowired
    private ValidateUpdatesMessageHandlingService updatesMessageHandlingService;

    private Thread shutdownHook;
    final CountDownLatch smileRequestFilterAppClose = new CountDownLatch(1);

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Starting up SMILE Request Filter application...");
        try {
            installShutdownHook();
            messagingGateway.connect();
            messageHandlingService.initialize(messagingGateway);
            updatesMessageHandlingService.initialize(messagingGateway);
            smileRequestFilterAppClose.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }

    private void installShutdownHook() {
        shutdownHook =
            new Thread() {
                public void run() {
                    System.err.printf("\nCaught CTRL-C, shutting down gracefully...\n");
                    try {
                        messagingGateway.shutdown();
                        messageHandlingService.shutdown();
                        updatesMessageHandlingService.shutdown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    smileRequestFilterAppClose.countDown();
                }
            };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public static void main(String[] args) {
        SpringApplication.run(SmileRequestFilterApp.class, args);
    }

}
