package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import org.mskcc.smile.service.RequestFilterMessageHandlingService;
import org.mskcc.smile.service.ValidRequestChecker;
import org.mskcc.smile.service.util.RequestStatusLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class RequestFilterMsgHandlingServiceIml implements RequestFilterMessageHandlingService {

    @Value("${igo.request_filter_topic}")
    private String IGO_REQUEST_FILTER_TOPIC;

    @Value("${igo.cmo_label_generator_topic}")
    private String CMO_LABEL_GENERATOR_TOPIC;

    @Value("${igo.new_request_topic}")
    private String IGO_NEW_REQUEST_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Autowired
    private ValidRequestChecker validRequestChecker;

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    private final ObjectMapper mapper = new ObjectMapper();
    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static final BlockingQueue<String> requestFilterQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch requestFilterHandlerShutdownLatch;
    private static Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(RequestFilterMsgHandlingServiceIml.class);

    private class RequestFilterHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        RequestFilterHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String requestJson = requestFilterQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestJson != null) {
                        String requestId = validRequestChecker.getRequestId(requestJson);
                        String filteredRequestJson = validRequestChecker.getFilteredValidRequestJson(
                                requestJson);
                        Boolean passCheck = (filteredRequestJson != null);

                        if (validRequestChecker.isCmo(requestJson)) {
                            LOG.info("Handling CMO-specific sanity checking...");
                            if (passCheck) {
                                LOG.info("Sanity check passed, publishing to: " + CMO_LABEL_GENERATOR_TOPIC);
                                messagingGateway.publish(requestId,
                                    CMO_LABEL_GENERATOR_TOPIC,
                                    filteredRequestJson);
                            } else {
                                // TODO: publish the cmo request error msg here
                                LOG.error("Sanity check failed on request: " + requestId);
                            }

                        } else {
                            LOG.info("Handling non-CMO request...");
                            if (passCheck) {
                                LOG.info("Sanity check passed, publishing to: " + IGO_NEW_REQUEST_TOPIC);
                                messagingGateway.publish(requestId,
                                        IGO_NEW_REQUEST_TOPIC,
                                        filteredRequestJson);
                            } else {
                                // TODO: publish the non-cmo request error msg here
                                LOG.error("Sanity check failed on request: " + requestId);
                            }
                        }
                    }
                    if (interrupted && requestFilterQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during request handling", e);
                }
            }
            requestFilterHandlerShutdownLatch.countDown();
        }
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupRequestFilterHandler(messagingGateway, this);
            initializeRequestFilterHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request.\n");
        }
    }

    @Override
    public void requestFilterHandler(String requestJson) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            requestFilterQueue.put(requestJson);
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
        requestFilterHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializeRequestFilterHandlers() throws Exception {
        requestFilterHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser requestFilterPhaser = new Phaser();
        requestFilterPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            requestFilterPhaser.register();
            exec.execute(new RequestFilterHandler(requestFilterPhaser));
        }
        requestFilterPhaser.arriveAndAwaitAdvance();
    }

    private void setupRequestFilterHandler(Gateway gateway,
            RequestFilterMessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(IGO_REQUEST_FILTER_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + IGO_REQUEST_FILTER_TOPIC);
                try {
                    String requestJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    messageHandlingService.requestFilterHandler(requestJson);
                } catch (Exception e) {
                    LOG.error("Exception during processing of request on topic: "
                            + IGO_REQUEST_FILTER_TOPIC, e);
                    try {
                        // TODO: Track parsing errors here
                        requestStatusLogger.logRequestStatus(message.toString(),
                                RequestStatusLogger.StatusType.REQUEST_PARSING_ERROR);
                    } catch (IOException ex) {
                        LOG.error("Error during attempt to write request status to logger file", ex);
                    }
                }
            }
        });
    }
}
