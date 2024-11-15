package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.mskcc.smile.service.PromotedRequestMsgHandlingService;
import org.mskcc.smile.service.ValidRequestChecker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class PromotedRequestMsgHandlingServiceImpl implements PromotedRequestMsgHandlingService {

    @Value("${igo.validate_promoted_request_topic:}")
    private String VALIDATE_PROMOTED_REQUEST_TOPIC;

    @Value("${igo.cmo_promoted_label_topic:}")
    private String CMO_PROMOTED_LABEL_TOPIC;

    @Value("${igo.promoted_request_topic:}")
    private String IGO_PROMOTED_REQUEST_TOPIC;

    @Value("${num.promoted_request_handler_threads:1}")
    private int NUM_PROMOTED_REQUEST_HANDLERS;

    @Autowired
    private ValidRequestChecker validRequestChecker;

    private final ObjectMapper mapper = new ObjectMapper();
    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private static final BlockingQueue<String> promotedRequestQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch promotedRequestHandlerShutdownLatch;
    private static Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(PromotedRequestMsgHandlingServiceImpl.class);

    private class PromotedRequestHandler implements Runnable {
        final Phaser phaser;
        boolean interrupted = false;

        PromotedRequestHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = promotedRequestQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        Map<String, Object> promotedRequestJsonMap =
                                validRequestChecker.generatePromotedRequestValidationMap(requestJson);
                        Map<String, Object> requestStatus =
                                mapper.convertValue(promotedRequestJsonMap.get("status"), Map.class);
                        String requestWithStatus = updateJsonWithValidationMap(requestJson, requestStatus);

                        if ((Boolean) requestStatus.get("validationStatus")) {
                            // if request is cmo then publish to CMO_PROMOTED_LABEL_TOPIC
                            // otherwise publish to IGO_PROMOTED_REQUEST_TOPIC
                            String topic = validRequestChecker.isCmo(requestJson)
                                    ? CMO_PROMOTED_LABEL_TOPIC : IGO_PROMOTED_REQUEST_TOPIC;
                            String requestId = validRequestChecker.getRequestId(requestJson);
                            LOG.info("Promoted request passed sanity checks - publishing to: " + topic);
                            messagingGateway.publish(requestId, topic, requestWithStatus);
                        }
                    }
                    if (interrupted && promotedRequestQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during request handling", e);
                }
            }
            promotedRequestHandlerShutdownLatch.countDown();
        }
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupPromotedRequestHandler(messagingGateway, this);
            initializePromotedRequestHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request.\n");
        }
    }

    @Override
    public void promotedRequestHandler(String requestJson) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            promotedRequestQueue.put(requestJson);
        } else {
            LOG.error("Shutdown initiated, not accepting request: " + requestJson);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        exec.shutdownNow();
        promotedRequestHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializePromotedRequestHandlers() throws Exception {
        promotedRequestHandlerShutdownLatch = new CountDownLatch(NUM_PROMOTED_REQUEST_HANDLERS);
        final Phaser promotedRequestPhaser = new Phaser();
        promotedRequestPhaser.register();
        for (int lc = 0; lc < NUM_PROMOTED_REQUEST_HANDLERS; lc++) {
            promotedRequestPhaser.register();
            exec.execute(new PromotedRequestHandler(promotedRequestPhaser));
        }
        promotedRequestPhaser.arriveAndAwaitAdvance();
    }

    private void setupPromotedRequestHandler(Gateway gateway,
            PromotedRequestMsgHandlingServiceImpl messageHandlingService)
            throws Exception {
        gateway.subscribe(VALIDATE_PROMOTED_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + VALIDATE_PROMOTED_REQUEST_TOPIC);
                try {
                    String requestJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    messageHandlingService.promotedRequestHandler(requestJson);
                } catch (Exception e) {
                    LOG.error("Exception during processing of request on topic: "
                            + VALIDATE_PROMOTED_REQUEST_TOPIC, e);
                }
            }
        });
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
