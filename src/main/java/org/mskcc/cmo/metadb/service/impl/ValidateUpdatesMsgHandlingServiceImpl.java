package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.metadb.service.ValidRequestChecker;
import org.mskcc.cmo.metadb.service.util.RequestStatusLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.mskcc.cmo.metadb.service.ValidateUpdatesMessageHandlingService;

@Service
public class ValidateUpdatesMsgHandlingServiceImpl implements ValidateUpdatesMessageHandlingService {
    @Value("${igo.validate_request_update_topic}")
    private String VALIDATOR_REQUEST_UPDATE_TOPIC;

    @Value("${igo.validate_sample_update_topic}")
    private String VALIDATOR_SAMPLE_UPDATE_TOPIC;

    @Value("${igo.cmo_label_update_topic}")
    private String CMO_LABEL_UPDATE_TOPIC;

    @Value("${smile.request_update_topic}")
    private String SERVER_REQUEST_UPDATE_TOPIC;

    @Value("${smile.sample_update_topic")
    private String SERVER_SAMPLE_UPDATE_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Autowired
    private ValidRequestChecker validRequestChecker;

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    private static boolean initialized = false;
    private static Gateway messagingGateway;
    private static final Log LOG = LogFactory.getLog(MessageHandlingServiceImpl.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static volatile boolean shutdownInitiated;

    private static CountDownLatch requestUpdateFilterHandlerShutdownLatch;
    private static final BlockingQueue<String> requestUpdateFilterQueue =
            new LinkedBlockingQueue<String>();

    private static CountDownLatch sampleUpdateFilterHandlerShutdownLatch;
    private static final BlockingQueue<String> sampleUpdateFilterQueue =
            new LinkedBlockingQueue<String>();

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupRequestUpdateFilterHandler(messagingGateway, this);
            setupSampleUpdateFilterHandler(messagingGateway, this);
            initializeMessageFilterHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized,"
                    + "ignoring request or sample updates.\n");
        }
    }

    private void initializeMessageFilterHandlers() throws Exception {
        requestUpdateFilterHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser requestFilterPhaser = new Phaser();
        requestFilterPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            requestFilterPhaser.register();
            exec.execute(new RequestUpdateFilterHandler(requestFilterPhaser));
        }
        requestFilterPhaser.arriveAndAwaitAdvance();

        sampleUpdateFilterHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser sampleFilterPhaser = new Phaser();
        sampleFilterPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            sampleFilterPhaser.register();
            exec.execute(new SampleUpdateFilterHandler(sampleFilterPhaser));
        }
        sampleFilterPhaser.arriveAndAwaitAdvance();
    }

    private class RequestUpdateFilterHandler implements Runnable {
        final Phaser phaser;
        boolean interrupted = false;

        RequestUpdateFilterHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = requestUpdateFilterQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        Boolean passCheck = validRequestChecker.isValidRequestMetadataJson(requestJson);

                        if (passCheck) {
                            LOG.info("Sanity check passed, publishing to: " + SERVER_REQUEST_UPDATE_TOPIC);
                            messagingGateway.publish(
                                    SERVER_REQUEST_UPDATE_TOPIC,
                                    requestJson);
                        } else {
                            LOG.error("Sanity check failed on Request updates: "
                                    + validRequestChecker.getRequestId(requestJson));
                            requestStatusLogger.logRequestStatus(requestJson,
                                    RequestStatusLogger.StatusType.REQUEST_UPDATE_FAILED_SANITY_CHECK);
                        }
                    }
                    if (interrupted && requestUpdateFilterQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Encountered error during handling of Request Metadata updates.", e);
                }
            }
            requestUpdateFilterHandlerShutdownLatch.countDown();
        }
    }

    private class SampleUpdateFilterHandler implements Runnable {
        final Phaser phaser;
        boolean interrupted = false;

        SampleUpdateFilterHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String sampleJson = sampleUpdateFilterQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (sampleJson != null) {
                        Map<String, String> sampleMap = mapper.readValue(sampleJson, Map.class);
                        Boolean isCmoSample = validRequestChecker.isCmo(sampleJson);
                        Boolean hasRequestId = validRequestChecker.hasRequestId(sampleJson);

                        if (isCmoSample) {
                            if (validRequestChecker.isValidCmoSample(sampleMap, hasRequestId)) {
                                LOG.info("Sanity check passed, publishing CMO sample"
                                        + "update to: " + CMO_LABEL_UPDATE_TOPIC);
                                messagingGateway.publish(CMO_LABEL_UPDATE_TOPIC,
                                        sampleJson);
                            } else {
                                LOG.error("Sanity check failed on CMO sample updates ");
                                requestStatusLogger.logRequestStatus(sampleJson,
                                        RequestStatusLogger.StatusType.SAMPLE_UPDATE_FAILED_SANITY_CHECK);
                            }
                        } else {
                            if (validRequestChecker.isValidNonCmoSample(sampleMap)) {
                                LOG.info("Sanity check passed, publishing non-CMO "
                                        + "sample update to: " + SERVER_SAMPLE_UPDATE_TOPIC);
                                messagingGateway.publish(
                                        SERVER_SAMPLE_UPDATE_TOPIC,
                                        sampleJson);
                            } else {
                                LOG.error("Sanity check failed on non-CMO sample update received.");
                                requestStatusLogger.logRequestStatus(sampleJson,
                                        RequestStatusLogger.StatusType.SAMPLE_UPDATE_FAILED_SANITY_CHECK);
                            }
                        }
                    }
                    if (interrupted && sampleUpdateFilterQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Encountered error during handling of Sample Metadata updates.", e);
                }
            }
            sampleUpdateFilterHandlerShutdownLatch.countDown();
        }
    }

    @Override
    public void requestUpdateFilterHandler(String requestJson) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            requestUpdateFilterQueue.put(requestJson);
        } else {
            LOG.error("Shutdown initiated, not accepting request: " + requestJson);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void sampleUpdateFilterHandler(String sampleJson) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            sampleUpdateFilterQueue.put(sampleJson);
        } else {
            LOG.error("Shutdown initiated, not accepting sample: " + sampleJson);
            throw new IllegalStateException("Shutdown initiated, not handling any more samples");
        }
    }

    private void setupRequestUpdateFilterHandler(Gateway gateway,
            ValidateUpdatesMessageHandlingService updateMessageHandlingService)
            throws Exception {
        gateway.subscribe(VALIDATOR_REQUEST_UPDATE_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + VALIDATOR_REQUEST_UPDATE_TOPIC);
                try {
                    String requestJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    updateMessageHandlingService.requestUpdateFilterHandler(requestJson);
                } catch (Exception e) {
                    LOG.error("Exception during processing of Request Metadata update on topic: "
                            + VALIDATOR_REQUEST_UPDATE_TOPIC, e);
                }
            }
        });
    }

    private void setupSampleUpdateFilterHandler(Gateway gateway,
            ValidateUpdatesMessageHandlingService updateMessageHandlingService)
            throws Exception {
        gateway.subscribe(VALIDATOR_SAMPLE_UPDATE_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + VALIDATOR_SAMPLE_UPDATE_TOPIC);
                try {
                    String sampleJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    updateMessageHandlingService.sampleUpdateFilterHandler(sampleJson);
                } catch (Exception e) {
                    LOG.error("Exception during processing of Sample Metadata update on topic: "
                            + VALIDATOR_SAMPLE_UPDATE_TOPIC, e);
                }
            }
        });
    }

    @Override
    public void shutdown() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        exec.shutdownNow();
        requestUpdateFilterHandlerShutdownLatch.await();
        sampleUpdateFilterHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }
}
