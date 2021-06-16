package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
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

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    @Value("${igo.cmo_request_filter:false}")
    private Boolean igoCmoRequestFilter;

    private static final Log LOG = LogFactory.getLog(ValidRequestCheckerImpl.class);

    /**
     * getFilteredValidRequestJson checks if the request is a cmoRequest and has a requestId
     * 
     * <p><ul>
     * <li>Sample list is extracted from the requestJsonMap
     * <li>If the total number of samples available is equal to valid samples,
     * then the whole request is valid
     * <li>If the number of valid samples is zero, the request is invalid and will be logged
     * <li>If the number of valid samples is less than the total number of samples, the
     * request is still considered valid but logged to keep note of the invalid samples
     * </ul>
     * @throws IOException
     */
    @Override
    public String getFilteredValidRequestJson(String requestJson) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        if (StringUtils.isAllBlank(requestJson)) {
            return null;
        }

        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        boolean isCmoRequest = isCmoRequest(requestJsonMap);
        boolean hasRequestId = hasRequestId(requestJsonMap);

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
            } else if (isValidCmoSample(sampleMap, isCmoRequest, hasRequestId)) {
                    validSampleList.add(sample);
            }

        }

        if (validSampleList.size() > 0) {
            if (validSampleList.size() < numOfSamples) {
                String newSamplesString = mapper.writeValueAsString(validSampleList.toArray(new Object[0]));
                requestJsonMap.replace("samples", newSamplesString);
                LOG.info("CMO request passed sanity checking - missing samples...");
                requestStatusLogger.logRequestStatus(requestJson,
                        RequestStatusLogger.StatusType.CMO_REQUEST_MISSING_REQ_FIELDS);
            }
            return mapper.writeValueAsString(requestJsonMap);
        }
        LOG.error("Request failed sanity checking - logging request status...");
        requestStatusLogger.logRequestStatus(requestJson,
                RequestStatusLogger.StatusType.CMO_REQUEST_FAILED_SANITY_CHECK);
        return null;
    }

    /**
     * isValidCmoSample receives a sampleMap
     * 
     * <p><ul>
     * <li>Checks if the sample has all the required fields
     * <li>If RequestId, InvestigatorSampleId, CmoPatientId,
     * SpecimenType, SampleType or NormalizedPatientId are missing, returns false
     * </ul>
     * @throws JsonProcessingException, JsonMappingException 
     */
    private boolean isValidCmoSample(Map<String, String> sampleMap,
            boolean isCmoRequest, boolean hasRequestId) throws JsonMappingException, JsonProcessingException {
        if (sampleMap.isEmpty() || sampleMap == null) {
            return Boolean.FALSE;
        }
        if (isCmoRequest && !hasBaitSet(sampleMap)) {
            return Boolean.FALSE;
        }
        if (!hasRequestId || !hasInvestigatorSampleId(sampleMap)
                || !hasCmoPatientId(sampleMap) || !hasSpecimenType(sampleMap)
                || !hasSampleType(sampleMap) || !hasNormalizedPatientId(sampleMap)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * isValidNonCmoSample receives a sampleMap
     * 
     * <p><ul>
     * <li>Check if the sample has all the required fields
     * <li>If baitSet or normalizedPatientId is missing, returns false
     * </ul>
     * @throws JsonProcessingException, JsonMappingException 
     */
    private boolean isValidNonCmoSample(Map<String, String> sampleMap) throws JsonMappingException, JsonProcessingException {
        if (sampleMap.isEmpty()
                || sampleMap == null
                || !hasBaitSet(sampleMap)
                || !hasNormalizedPatientId(sampleMap)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private boolean isCmoRequest(Map<String, Object> requestJsonMap) {
        if (Strings.isBlank(requestJsonMap.get("cmoRequest").toString())) {
            return Boolean.FALSE;
        }
        return Boolean.valueOf(requestJsonMap.get("cmoRequest").toString());
    }

    private boolean hasRequestId(Map<String, Object> requestJsonMap) {
        return Strings.isBlank(requestJsonMap.get("requestId").toString());
    }

    private boolean hasBaitSet(Map<String, String> sampleMap) {
        return Strings.isBlank(sampleMap.get("baitSet"));

    }

    private boolean hasInvestigatorSampleId(Map<String, String> sampleMap) {
        return Strings.isBlank(sampleMap.get("investigatorSampleId"));
    }

    private boolean hasCmoPatientId(Map<String, String> sampleMap) {
        return Strings.isBlank(sampleMap.get("cmoPatientId"));
    }

    /**
     * hasSpecimenType ensures all variables required to retrieve
     * Sample Type Abbreviation are available
     * 
     * <p><ul>
     * <li>if the specimenType is blank or not one of the listed types
     * then we look for CmoSampleClass
     * <li>if it "exosome" and "cfDna" then we look for sampleOrigin
     * </ul>
     * @param sampleMap
     * @return
     */
    private boolean hasSpecimenType(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("specimenType"))
                || !EnumUtils.isValidEnumIgnoreCase(SpecimenType.class, sampleMap.get("specimenType"))) {
            return hasCmoSampleClass(sampleMap);
        }
        // If the specimen type isn't CellLine, PDX, Xenograft, XenograftDerivedCellLine or Organoid
        // use CmoSampleClass
        if (!sampleMap.get("specimenType").equalsIgnoreCase("CellLine")
                || !sampleMap.get("specimenType").equalsIgnoreCase("PDX")
                || !sampleMap.get("specimenType").equalsIgnoreCase("Xenograft")
                || !sampleMap.get("specimenType").equalsIgnoreCase("XenograftDerivedCellLine")
                || !sampleMap.get("specimenType").equalsIgnoreCase("Organoid")) {
            return hasCmoSampleClass(sampleMap);
        }
        // If the specimen type is exosome or cfDNA, use sampleOrigin
        if (sampleMap.get("specimenType").equalsIgnoreCase("exosome")
                || sampleMap.get("specimenType").equalsIgnoreCase("cfDNA")) {
            return hasSampleOrigin(sampleMap);
        }
        return Boolean.TRUE;
    }

    private boolean hasCmoSampleClass(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("cmoSampleClass"))
                || !EnumUtils.isValidEnumIgnoreCase(CmoSampleClass.class,
                        sampleMap.get("cmoSampleClass"))) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private boolean hasSampleOrigin(Map<String, String> sampleMap) {
        if (Strings.isBlank(sampleMap.get("sampleOrigin"))
                || !EnumUtils.isValidEnumIgnoreCase(SampleOrigin.class, sampleMap.get("sampleOrigin"))) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * hasSampleType ensures all variables required to retrieve
     * Nucleid Acid Abbreviation are available
     * 
     * <p><ul>
     * <li>if sampleType is null empty, checks NAtoExtract
     * <li>if sampleType is "Pooled Library", checks recipe
     * <li>Else returns true if sampleType has a valid value
     * </ul>
     * @param sampleMap
     * @return
     * @throws JsonProcessingException, JsonMappingException 
     */
    private boolean hasSampleType(Map<String, String> sampleMap) throws JsonMappingException, JsonProcessingException {
        if (Strings.isBlank(sampleMap.get("sampleType"))) {
            return hasNAtoExtract(sampleMap);
        }
        if (sampleMap.get("sampleType").equalsIgnoreCase("Pooled Library")) {
            return hasRecipe(sampleMap);
        }
        if (EnumUtils.isValidEnumIgnoreCase(SampleType.class, sampleMap.get("sampleType"))) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    private boolean hasRecipe(Map<String, String> sampleMap) {
        return Strings.isBlank(sampleMap.get("recipe"));
    }

    private boolean hasNAtoExtract(Map<String, String> sampleMap) throws JsonMappingException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdfields = mapper.readValue(sampleMap.get("cmoSampleIdFields"), Map.class);
            return Strings.isBlank(cmoSampleIdfields.get("naToExtract"));
        }
        return Boolean.FALSE;
    }
    
    private boolean hasNormalizedPatientId(Map<String, String> sampleMap) throws JsonMappingException, JsonProcessingException {   
        ObjectMapper mapper = new ObjectMapper();
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdfields = mapper.readValue(sampleMap.get("cmoSampleIdFields"), Map.class);
            return Strings.isBlank(cmoSampleIdfields.get("normalizedPatientId"));
        }
        return Boolean.FALSE;
    }
}
