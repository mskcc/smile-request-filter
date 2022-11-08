package org.mskcc.smile.service.util;

import java.util.List;

public class RequestErrorMsg {
    // Maybe change the name to requestStatus?
    private String date;
    // Could be useful in the future
    private String Status;
    private List<SampleErrorMsg> sampleErrorMsgList;
    
    
    public enum StatusType {
        REQUEST_PASSED,
        REQUEST_PARSING_ERROR,
        REQUEST_IS_NULL_OR_EMPTY,
        REQUEST_WITH_SAMPLES_MISSING_FIELDS,
        CMO_REQUEST_FILTER_SKIPPED_REQUEST,
        CMO_REQUEST_MISSING_REQUESTID,
        PERSITED_REQUEST_WITH_MISSING_SAMPLES
    }
    // Should we look through the samples if the request metadata is invalid?
    // RequestMetadata would be considered invalid if:
    // it is null or empty
    // if it is missing requestId
    // if cmo-filter is true and the request is non-cmo
    // if the request has no samples
}