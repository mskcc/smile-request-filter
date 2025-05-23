package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
import org.mskcc.smile.service.ValidRequestChecker;
import org.mskcc.smile.service.ValidateUpdatesMessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ValidateUpdatesMsgHandlingServiceImpl implements ValidateUpdatesMessageHandlingService {
    @Value("${igo.validate_request_update_topic:}")
    private String VALIDATOR_REQUEST_UPDATE_TOPIC;

    @Value("${igo.validate_sample_update_topic:}")
    private String VALIDATOR_SAMPLE_UPDATE_TOPIC;

    @Value("${igo.cmo_label_update_topic:}")
    private String CMO_LABEL_UPDATE_TOPIC;

    @Value("${smile.request_update_topic:}")
    private String SERVER_REQUEST_UPDATE_TOPIC;

    @Value("${smile.sample_update_topic:}")
    private String SERVER_SAMPLE_UPDATE_TOPIC;

    @Value("${num.new_request_handler_threads:1}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Autowired
    private ValidRequestChecker validRequestChecker;

    private static boolean initialized = false;
    private static Gateway messagingGateway;
    private static final Log LOG = LogFactory.getLog(RequestFilterMsgHandlingServiceIml.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static volatile boolean shutdownInitiated;

    private static CountDownLatch requestUpdateFilterHandlerShutdownLatch;
    private static final BlockingQueue<String> requestUpdateFilterQueue =
            new LinkedBlockingQueue<>();

    private static CountDownLatch sampleUpdateFilterHandlerShutdownLatch;
    private static final BlockingQueue<List<Object>> sampleUpdateFilterQueue =
            new LinkedBlockingQueue<>();

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
                        String requestId = validRequestChecker.getRequestId(requestJson);
                        Map<String, Object> requestStatus =
                                validRequestChecker.generateRequestStatusValidationMap(requestJson);
                        // attach updated request status to the request metadata
                        String requestWithStatus = updateJsonWithValidationMap(requestJson, requestStatus);

                        Boolean passCheck = (Boolean) requestStatus.get("validationStatus");
                        if (passCheck) {
                            LOG.info("Sanity check passed for request updates: " + requestId);
                        } else {
                            LOG.error("Sanity check failed on request updates: " + requestWithStatus);
                        }
                        messagingGateway.publish(
                                SERVER_REQUEST_UPDATE_TOPIC,
                                requestWithStatus);
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
                    List<Object> sampleJsonList = sampleUpdateFilterQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (sampleJsonList != null) {
                        List<String> cmoSamples = new ArrayList<>();
                        List<String> nonCmoSamples = new ArrayList<>();

                        for (int i = 0; i < sampleJsonList.size(); i++) {
                            String sampleJson = mapper.writeValueAsString(sampleJsonList.get(i));

                            Map<String, Object> sampleMap = mapper.readValue(sampleJson, Map.class);
                            Boolean hasRequestId = validRequestChecker.hasRequestId(sampleJson);
                            if (!hasRequestId) {
                                LOG.warn("Cannot extract request ID information from sample update message: "
                                        + sampleJson);
                                continue;
                            }

                            Boolean isCmoSample = validRequestChecker.isCmo(sampleJson);
                            if (isCmoSample) {
                                Map<String, Object> sampleStatus =
                                        validRequestChecker.generateCmoSampleValidationMap(sampleMap);
                                // attach sample status to sample json to publish
                                String sampleWithStatus
                                        = updateJsonWithValidationMap(sampleJson, sampleStatus);

                                Boolean passCheck = (Boolean) sampleStatus.get("validationStatus");
                                if (passCheck) {
                                    LOG.info("Sanity check passed, publishing CMO sample"
                                            + "update to: " + CMO_LABEL_UPDATE_TOPIC);
                                } else {
                                    LOG.error("Sanity check failed on CMO sample updates: "
                                            + sampleWithStatus);
                                }
                                cmoSamples.add(sampleWithStatus);
                            } else {
                                Map<String, Object> sampleStatus =
                                        validRequestChecker.generateNonCmoSampleValidationMap(sampleMap);
                                // attach sample status to sample json to publish
                                String sampleWithStatus 
                                        = updateJsonWithValidationMap(sampleJson, sampleStatus);

                                Boolean passCheck = (Boolean) sampleStatus.get("validationStatus");
                                if (passCheck) {
                                    LOG.info("Sanity check passed, publishing non-CMO "
                                            + "sample update to: " + SERVER_SAMPLE_UPDATE_TOPIC);
                                } else {
                                    LOG.error("Sanity check failed on non-CMO sample update received: "
                                            + sampleWithStatus);
                                }
                                nonCmoSamples.add(sampleWithStatus);
                            }
                        }

                        // direct samples to label generator or smile server based on cmo status
                        // handle the possibility that there could be a mix of both cmo and non-cmo samples
                        if (!cmoSamples.isEmpty()) {
                            messagingGateway.publish(CMO_LABEL_UPDATE_TOPIC,
                                            cmoSamples);
                        }
                        if (!nonCmoSamples.isEmpty()) {
                            messagingGateway.publish(
                                        SERVER_SAMPLE_UPDATE_TOPIC,
                                        nonCmoSamples);
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
    public void sampleUpdateFilterHandler(List<Object> sampleJsonList) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            sampleUpdateFilterQueue.put(sampleJsonList);
        } else {
            LOG.error("Shutdown initiated, not accepting samples: " + sampleJsonList);
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
                    Object msgDataObject = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            Object.class);
                    List<Object> sampleJsonList = mapper.readValue(msgDataObject.toString(), List.class);
                    updateMessageHandlingService.sampleUpdateFilterHandler(sampleJsonList);
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

    /**
     * Updates the input json with the validation map provided.
     * The validation map contains the validation report and validation status.
     * @param inputJson
     * @param validationMap
     * @return String
     * @throws JsonProcessingException
     */
    private String updateJsonWithValidationMap(String inputJson, Map<String, Object> validationMap)
            throws JsonProcessingException {
        Map<String, Object> inputJsonMap = mapper.readValue(inputJson, Map.class);
        inputJsonMap.put("status", validationMap);
        return mapper.writeValueAsString(inputJsonMap);
    }
}
