package org.mskcc.smile.service.util;

import java.util.ArrayList;
import java.util.List;

public class SampleErrorMsg {
    // This class may be move to commons later
    private String date;
    // Could be useful in the future
    private StatusType Status;
    private List<String> missingFieldList;
    private List<String> invalidFieldList;

    // CMO SAMPLES
    // investigatorId missing
    // igoId missing
    // cmoPatientId missing
    // baitSet or recipe
    // normalizedPatientId missing
    // fastQs missing
    // specimenType or sampleType
    
    // specimenType:
    // Case 1: if null/empty return cmoSampleClass
    // cmoSampleClass missing
    // Case 2: if it is CELLLINE, PDX, XENOGRAFT, XENOGRAFTDERIVEDCELLLINE or ORGANOID return cmoSampleClass
    // cmoSampleClass missing
    // Case 3: if it is EXOSOME or CFDNA return sampleOrigin
    // sampleOrigin missing
    // Case 4: invalid enum type
    // invalid specimenType
    // or
    // sampleType:
    // Case 1: if null/empty check NAtoExtract
    // NAtoExtract missing
    // Case 2: if it is POOLED_LIBRARY check baitSet
    //  baitSet missing --> do not log
    // Case 3: if it a valid enum
    // invalid sampleType
    
    
    // NON-CMO SAMPLES
    // baitSet missing
    // normalizedPatientId missing
    
    public enum StatusType {
        SAMPLE_PARSING_ERROR,
        SAMPLE_IS_NULL_OR_EMPTY,
        SAMPLES_MISSING_CMO_LABEL_FIELDS
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public StatusType getStatus() {
        return Status;
    }

    public void setStatus(StatusType status) {
        Status = status;
    }

    public List<String> getMissingFieldList() {
        return missingFieldList;
    }

    public void setMissingFieldList(List<String> missingFieldList) {
        this.missingFieldList = missingFieldList;
    }
    
    public void addMissingField(String fieldName) {
        if (missingFieldList == null) {
            missingFieldList = new ArrayList<>();
        }
        missingFieldList.add(fieldName); 
    }

    public List<String> getInvalidFieldList() {
        return invalidFieldList;
    }

    public void setInvalidFieldList(List<String> invalidFieldList) {
        this.invalidFieldList = invalidFieldList;
    }
    
    public void addInvalidField(String fieldName) {
        if (invalidFieldList == null) {
            invalidFieldList = new ArrayList<>();
        }
        invalidFieldList.add(fieldName);        
    }
}
