package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.util.Strings;
import org.mskcc.cmo.common.enums.CmoSampleClass;
import org.mskcc.cmo.common.enums.SampleOrigin;
import org.mskcc.cmo.common.enums.SampleType;
import org.mskcc.cmo.common.enums.SpecimenType;
import org.mskcc.cmo.metadb.service.ValidRequestChecker;
import org.mskcc.cmo.metadb.service.util.RequestStatusLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ValidRequestCheckerImpl implements ValidRequestChecker {

    private final ObjectMapper mapper = new ObjectMapper();
    private boolean isCmoRequest;
    private boolean hasRequestId;

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    @Value("${igo.cmo_request_filter:false}")
    private Boolean igoCmoRequestFilter;

    private static final Log LOG = LogFactory.getLog(ValidRequestCheckerImpl.class);

    /**
     * Checks is the request is a cmoRequest and has a requestId
     * Sample list is extracted from the requestJsonMap
     * If the total number of samples available is equal to valid samples,
     * then the request as a whole is valid
     * If the number of valid samples is zero, the request is invalid and will be logged
     * If the number of valid samples is less than the total number of samples, the
     * request is still considered valid but logged to keep note of the invalid samples
     * @throws IOException
     */
    @Override
    public String checkIfValidRequest(String requestJson) throws IOException {

        if (StringUtils.isAllBlank(requestJson)) {
            return null;
        }

        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        isCmoRequest = isCmoRequest(requestJsonMap);
        hasRequestId = hasRequestId(requestJsonMap);

        if (igoCmoRequestFilter && !isCmoRequest) {
            LOG.info("CMO request filter enabled - skipping non-CMO request: "
                    + requestJsonMap.get("requestId"));
            return null;
        }

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        List<Object> validSampleList = new ArrayList<Object>();
        int numOfSamples = sampleList.length;

        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            if (!igoCmoRequestFilter && !isCmoRequest) {
                if (isValidNonCmoSample(sampleMap)) {
                    validSampleList.add(sample);
                }
            } else {
                if (isValidCmoSample(sampleMap)) {
                    validSampleList.add(sample);
                }
            }

        }

        if (validSampleList.size() > 0) {
            if (validSampleList.size() < numOfSamples) {
                String newSamplesString = mapper.writeValueAsString(validSampleList.toArray(new Object[0]));
                requestJsonMap.replace("samples", newSamplesString);
                LOG.info("CMO request passed sanity checking - missing samples...");
                requestStatusLogger.logRequestStatus(requestJson,
                        RequestStatusLogger.StatusType.REQUEST_WITH_MISSING_SAMPLES);
            }
            return mapper.writeValueAsString(requestJsonMap);
        }
        LOG.error("Request failed sanity checking - logging request status...");
        requestStatusLogger.logRequestStatus(requestJson,
                RequestStatusLogger.StatusType.CMO_REQUEST_FAILED_SANITY_CHECK);
        return null;
    }

    /**
     * Receives a sampleMap
     * Check if the sample has all the required fields
     * If RequestId, InvestigatorSampleId, CmoPatientId,
     * SpecimenType, SampleType or NormalizedPatientId are missing, returns false
     */
    private boolean isValidCmoSample(Map<String, String> sampleMap) {
        if (sampleMap.isEmpty() || sampleMap == null) {
            return false;
        }
        if (isCmoRequest && !hasBaitSet(sampleMap)) {
            return false;
        }
        if (!hasRequestId || !hasInvestigatorSampleId(sampleMap)
                || !hasCmoPatientId(sampleMap) || !hasSpecimenType(sampleMap)
                || !hasSampleType(sampleMap) || !hasNormalizedPatientId(sampleMap)) {
            return false;
        }
        return true;
    }

    /**
     * Receives a sampleMap
     * Check if the sample has all the required fields
     * If baitSet or cmoPatientId is missing, returns false
     */
    private boolean isValidNonCmoSample(Map<String, String> sampleMap) {
        if (sampleMap.isEmpty() || sampleMap == null) {
            return false;
        }
        if (!hasBaitSet(sampleMap) || !hasNormalizedPatientId(sampleMap)) {
            return false;
        }
        return true;
    }

    private boolean isCmoRequest(Map<String, Object> requestJsonMap) {
        if (Strings.isBlank(requestJsonMap.get("cmoRequest").toString())) {
            return false;
        }
        return true;
    }

    private boolean hasRequestId(Map<String, Object> requestJsonMap) {
        if (Strings.isBlank(requestJsonMap.get("requestId").toString())) {
            return false;
        }
        return true;
    }

    private boolean hasBaitSet(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("baitSet"))) {
            return false;
        }
        return true;
    }

    private boolean hasInvestigatorSampleId(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("investigatorSampleId"))) {
            return false;
        }
        return true;
    }

    private boolean hasCmoPatientId(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("cmoPatientId"))) {
            return false;
        }
        return true;
    }

    /**
     * Ensures all variables required to retrieve Sample Type Abbreviation are available
     * if the specimenType is blank or not one of the listed types then we look for CmoSampleClass
     * if it "exosome" and "cfDna" then we look for sampleOrigin
     * @param sampleMap
     * @return
     */
    private boolean hasSpecimenType(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("specimenType"))
                || !EnumUtils.isValidEnumIgnoreCase(SpecimenType.class, sampleMap.get("specimenType"))) {
            return hasCmoSampleClass(sampleMap);
        }
        if (sampleMap.get("specimenType") != "CellLine"
                || sampleMap.get("specimenType") != "PDX"
                || sampleMap.get("specimenType") != "Xenograft"
                || sampleMap.get("specimenType") != "XenograftDerivedCellLine"
                || sampleMap.get("specimenType") != "Organoid") {
            return hasCmoSampleClass(sampleMap);
        }
        if (sampleMap.get("specimenType") == "exosome"
                || sampleMap.get("specimenType") == "cfDNA") {
            return hasSampleOrigin(sampleMap);
        }
        return true;
    }

    private boolean hasCmoSampleClass(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("cmoSampleClass"))) {
            return false;
        }
        if (!EnumUtils.isValidEnumIgnoreCase(CmoSampleClass.class, sampleMap.get("cmoSampleClass"))) {
            return false;
        }
        return true;
    }

    private boolean hasSampleOrigin(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("sampleOrigin"))) {
            return false;
        }
        if (EnumUtils.isValidEnumIgnoreCase(SampleOrigin.class, sampleMap.get("sampleOrigin"))) {
            return true;
        }
        return false;
    }

    /**
     * Ensures all variables required to retrieve Nucleid Acid Abbreviation are available
     * if sampleType is null empty, checks NAtoExtract
     * if sampleType is "Pooled Library", checks recipe
     * Else returns true if sampleType has a valid value
     * @param sampleMap
     * @return
     */
    private boolean hasSampleType(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("sampleType"))) {
            return hasNAtoExtract(sampleMap);
        }
        if (sampleMap.get("sampleType") == "Pooled Library") {
            return hasRecipe(sampleMap);
        }
        if (EnumUtils.isValidEnumIgnoreCase(SampleType.class, sampleMap.get("sampleType"))) {
            return true;
        }
        return false;
    }

    private boolean hasRecipe(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("recipe"))) {
            return false;
        }
        return true;
    }

    /**
     * missing this field from sampleManifest, returns true for testing
     * @param sampleMap
     * @return
     */
    private boolean hasNAtoExtract(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("NAtoExtract"))) {
            return true;
        }
        return true;
    }

    /**
     * missing this field from sampleManifest, returns true for testing
     * @param sampleMap
     * @return
     */
    private boolean hasNormalizedPatientId(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("normalizedPatientId"))) {
            return true;
        }
        return true;
    }
}
