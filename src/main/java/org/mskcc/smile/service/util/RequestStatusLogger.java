package org.mskcc.smile.service.util;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang3.StringUtils;
import org.mskcc.smile.commons.FileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Class for logging request statuses to the provided
 * Request Handling filepath.
 * @author ochoaa
 */
@Component
public class RequestStatusLogger {
    @Value("${req_filter.request_logger_filepath}")
    private String requestLoggerFilepath;

    @Autowired
    private FileUtil fileUtil;

    private File requestStatusLoggerFile;

    @Autowired
    private static final String[] REQUEST_LOGGER_FILE_HEADER = new String[]{"DATE", "STATUS", "MESSAGE"};

    /**
     * Request StatusType descriptions:
     * - REQUEST_WITH_MISSING_SAMPLES: a request that came in with no sample metadata
     * - CMO_REQUEST_WITH_SAMPLES_MISSING_CMO_LABEL_FIELDS: a CMO request with sample metadata
     *        that is missing required fields (cmoPatientId, baitSet)
     * - REQUEST_PARSING_ERROR: json parsing exception thrown
     * - CMO_REQUEST_FILTER_SKIPPED_REQUEST: applies if smile server is running
     *        with the cmoRequestFilter enabled and a non-cmo request is encountered
     * - CMO_REQUEST_FAILED_SANITY_CHECK: request failed cmo-specific sanity check on
     *        required fields for generating cmo labels
     * - REQUEST_UPDATE_FAILED_SANITY_CHECK: request metadata update failed sanity check on
     *        required fields
     * - SAMPLE_UPDATE_FAILED_SANITY_CHECK: sample metadata update failed sanity check on
     *        required fields. This can be for either cmo samples missing required
     *        fields to generate cmo label or non-cmo samples
     * - PROMOTED_REQUEST_FAILED_SANITY_CHECK: a promoted request which has failed sanity
     *        check on the minimally required fields
     * - PROMOTED_SAMPLES_MISSING_IDS: at least one sample in the promoted request is missing
     *        key identifier information (igoId and/or primaryId)
     */
    public enum StatusType {
        REQUEST_WITH_MISSING_SAMPLES,
        CMO_REQUEST_WITH_SAMPLES_MISSING_CMO_LABEL_FIELDS,
        REQUEST_PARSING_ERROR,
        CMO_REQUEST_FILTER_SKIPPED_REQUEST,
        CMO_REQUEST_FAILED_SANITY_CHECK,
        REQUEST_UPDATE_FAILED_SANITY_CHECK,
        SAMPLE_UPDATE_FAILED_SANITY_CHECK,
        PROMOTED_REQUEST_FAILED_SANITY_CHECK,
        PROMOTED_SAMPLES_MISSING_IDS
    }

    /**
     * Writes request contents and status to the request status logger file.
     * @param message
     * @param status
     * @throws IOException
     */
    public void logRequestStatus(String message, StatusType status) throws IOException {
        if (requestStatusLoggerFile ==  null) {
            this.requestStatusLoggerFile = fileUtil.getOrCreateFileWithHeader(requestLoggerFilepath,
                    StringUtils.join(REQUEST_LOGGER_FILE_HEADER, "\t") + "\n");
        }
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(currentDate)
                .append("\t")
                .append(status.toString())
                .append("\t")
                .append(message)
                .append("\n");
        fileUtil.writeToFile(requestStatusLoggerFile, builder.toString());
    }
}
