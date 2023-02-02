package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.util.Strings;
import org.mskcc.smile.commons.enums.CmoSampleClass;
import org.mskcc.smile.commons.enums.SampleOrigin;
import org.mskcc.smile.commons.enums.SampleType;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.service.ValidRequestChecker;
import org.mskcc.smile.service.util.RequestStatusLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ValidRequestCheckerImpl implements ValidRequestChecker {

    @Autowired
    private RequestStatusLogger requestStatusLogger;

    @Value("${igo.cmo_request_filter:false}")
    private Boolean igoCmoRequestFilter;

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(ValidRequestCheckerImpl.class);

    /**
     * Checks if the request is a cmoRequest and has a requestId, and returns a
     * filtered request JSON.
     * - a filtered JSON implies that some samples have been removed which were
     *   considered invalid
     *
     * <p>Extracts the sample list from the request JSON.
     * - If total number of samples available matches the number of valid samples then the
     *   whole request is considered valid.
     * - If the number of valid samples is zero, the request is invalid and will be logged.
     * - If the number of valid samples is less than the total number of samples that came
     *   with the request then the request is still considered valid but the request is
     *   logged by the request status logger to keep note of the invalid samples.
     * @throws IOException
     */
    @Override
    public String getFilteredValidRequestJson(String requestJson) throws IOException {
        // get request status report for request-level metadata
        Map<String, Object> requestStatus = generateRequestStatusValidationMap(requestJson);
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);

        // validate each sample json and add to validSampleList if it passes check
        Boolean isCmoRequest = isCmo(requestJsonMap);
        List<Object> updatedSampleList = new ArrayList<>();
        int validSampleCount = 0;
        for (Object sample: sampleList) {
            Map<String, Object> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus;
            if (isCmoRequest) {
                sampleStatus = generateCmoSampleValidationMap(sampleMap);
            } else {
                sampleStatus = generateNonCmoSampleValidationMap(sampleMap);
            }
            if ((Boolean) sampleStatus.get("validationStatus")) {
                validSampleCount++;
            }
            sampleMap.put("status", sampleStatus);
            updatedSampleList.add(mapper.convertValue(sampleMap, Object.class));
        }
        // update request status 'validationStatus' based on results from sample validation
        if ((Boolean) requestStatus.get("validationStatus")) {
            List<String> requestValidationReport = (List<String>) requestStatus.get("validationReport");
            if (validSampleCount == 0) {
                requestValidationReport.add("all request samples failed validation");
                requestStatus.replace("validationStatus", Boolean.FALSE);
            } else if (validSampleCount < sampleList.length) {
                requestValidationReport.add("some request samples failed validation");
            }
            requestStatus.replace("validationReport", requestValidationReport);
        }
        // update request json with request status and samples containing validation reports
        requestJsonMap.put("status", requestStatus);
        requestJsonMap.replace("samples", updatedSampleList.toArray(new Object[0]));
        return mapper.writeValueAsString(requestJsonMap);
    }

    @Override
    public Map<String, Object> generatePromotedRequestValidationMap(String requestJson)
            throws JsonMappingException, JsonProcessingException, IOException {
        // first check if request-level metadata is valid
        Map<String, Object> requestStatus = generateRequestStatusValidationMap(requestJson);
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);

        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        List<Object> updatedSampleList = new ArrayList<>();
        int validPromotedSampleCount = 0;
        for (Object sample: sampleList) {
            Map<String, Object> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus = generatePromotedSampleValidationMap(sampleMap);
            sampleMap.put("status", sampleStatus);
            updatedSampleList.add(sampleMap);
            if ((Boolean) sampleStatus.get("validationStatus")) {
                validPromotedSampleCount++;
            }
        }
        if (validPromotedSampleCount < sampleList.length) {
            LOG.warn("One or more sample(s) is missing one or a combination of the following: igoId or "
                    + "primaryId, cmoPatientId or cmoSampleIdFields --> normalizedPatientId - this "
                    + "information must be added for promoted requests & samples");
            requestStatusLogger.logRequestStatus(requestJson,
                RequestStatusLogger.StatusType.PROMOTED_SAMPLES_MISSING_IDS);
        }
        if (validPromotedSampleCount == 0) {
            requestStatus.put("validationStatus", Boolean.FALSE);
            List<String> requestValidationReport = (List<String>) requestStatus.get("validationReport");
            requestValidationReport.add("all samples in promoted request failed validation");
            requestStatus.replace("validationReport", requestValidationReport);
        }
        requestJsonMap.put("status", requestStatus);
        requestJsonMap.replace("samples", updatedSampleList.toArray(new Object[0]));
        return requestJsonMap;
    }

    @Override
    public Map<String, Object> generatePromotedSampleValidationMap(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        Map<String, Object> validationMap = new HashMap<>();
        Map<String, String> validationReport = new HashMap<>();
        Boolean validationStatus = Boolean.TRUE;
        if (!hasIgoId(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("igoId", "missing");
        }
        if (!(hasCmoPatientId(sampleMap) || hasNormalizedPatientId(sampleMap))) {
            validationStatus = Boolean.FALSE;
            if (!hasCmoPatientId(sampleMap)) {
                validationReport.put("cmoPatientId", "missing");
            }
            if (!hasNormalizedPatientId(sampleMap)) {
                validationReport.put("normalizedPatientId", "missing from cmoSampleIdFields");
            }
        }
        // update contents of validation map to return
        validationMap.put("validationStatus", validationStatus);
        validationMap.put("validationReport", validationReport);
        return validationMap;
    }

    /**
     * Evaluates request metadata and returns a boolean based on whether the request data
     * passes all sanity checks.
     * @param requestJson
     * @return Map
     * @throws IOException
     */
    @Override
    public Map<String, Object> generateRequestStatusValidationMap(String requestJson)
            throws IOException {
        Map<String, Object> validationMap = new HashMap<>();
        List<String> validationReport = new ArrayList<>();
        if (StringUtils.isAllBlank(requestJson)) {
            validationMap.put("validationStatus", Boolean.FALSE);
            validationMap.put("validationReport", validationReport);
            return validationMap;
        }

        Boolean validationStatus = Boolean.TRUE;
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        boolean isCmoRequest = isCmo(requestJsonMap);
        boolean hasRequestId = hasRequestId(requestJsonMap);

        // if requestId is blank then nothing to do, return null
        if (!hasRequestId) {
            LOG.warn("CMO request failed sanity checking - missing requestId...");
            requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.REQUEST_MISSING_REQUEST_ID);
            validationReport.add("requestId or igoRequestId missing from metadata");
            validationStatus = Boolean.FALSE;
        }

        // if cmo filter is enabled then skip request if it is non-cmo
        if (igoCmoRequestFilter && !isCmoRequest) {
            LOG.warn("CMO request filter enabled - skipping non-CMO request: "
                    + getRequestId(requestJsonMap));
            requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.CMO_REQUEST_FILTER_SKIPPED_REQUEST);
            validationReport.add("smile cmo request filter enabled but isCmoRequest = false");
            validationStatus = Boolean.FALSE;
        }

        // determine whether request json has samples
        if (!requestHasSamples(requestJson)) {
            validationReport.add("request is missing 'samples' in json or 'samples' is an empty list");
            validationStatus = Boolean.FALSE;
        }

        // update contents of validation map to return
        validationMap.put("validationStatus", validationStatus);
        validationMap.put("validationReport", validationReport);
        return validationMap;
    }

    /**
     * Evaluates sample metadata and returns a boolean based on whether the sample data
     * passes all sanity checks.
     * - Checks if the sample has all the required fields for CMO label generation
     * - If any of the following are missing then returns false:
     *   -  request id
     *   - investigator sample id
     *   - cmo patient id
     *   - specimen type
     *   - sample type
     *   - normalized patient id
     * @param sampleMap
     * @return Map
     * @throws JsonMappingException
     * @throws JsonProcessingException or JsonMappingException
     */
    @Override
    public Map<String, Object> generateCmoSampleValidationMap(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        Map<String, Object> validationMap = new HashMap<>();
        Map<String, String> validationReport = new HashMap<>();
        if (sampleMap == null || sampleMap.isEmpty()) {
            validationReport.put("sampleMetadata", "sample metadata json is empty");
            validationMap.put("validationStatus", Boolean.FALSE);
            validationMap.put("validationReport", validationReport);
            return validationMap;
        }

        Boolean validationStatus = Boolean.TRUE;
        if (!hasInvestigatorSampleId(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("investigatorSampleId", "missing");
        }
        if (!hasIgoId(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("igoId", "missing");
        }
        if (!hasBaitSetOrRecipe(sampleMap)) {
            validationStatus = Boolean.FALSE;
            if (!hasBaitSet(sampleMap)) {
                validationReport.put("baitSet", "missing");
            }
            if (!hasRecipe(sampleMap)) {
                validationReport.put("recipe", "missing");
            }
        }

        if (!hasCmoPatientId(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("cmoPatientId", "missing");
        }
        if (!(hasValidSpecimenType(sampleMap) || hasSampleType(sampleMap))) {
            validationStatus = Boolean.FALSE;
            if (!hasValidSpecimenType(sampleMap)) {
                validationReport.put("specimenType (sampleClass)", "invalid");
            }
            if (!hasSampleType(sampleMap)) {
                validationReport.put("sampleType", "missing from 'cmoSampleIdFields'");
            }
        }
        if (!hasNormalizedPatientId(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("normalizedPatientId", "missing from 'cmoSampleIdFields'");
        }
        if (!hasFastQs(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("fastQs", "missing");
        }
        validationMap.put("validationStatus", validationStatus);
        validationMap.put("validationReport", validationReport);
        return validationMap;
    }

    /**
     * Evaluates sample metadata for samples from NON-CMO requests.
     * - Checks if sample map has all required fields.
     * - If bait set or normalized patient id are  missing then returns false.
     * @param sampleMap
     * @return Map
     * @throws JsonMappingException
     * @throws JsonProcessingException or JsonMappingException
     */
    @Override
    public Map<String, Object> generateNonCmoSampleValidationMap(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        Map<String, Object> validationMap = new HashMap<>();
        Map<String, String> validationReport = new HashMap<>();
        if (sampleMap == null || sampleMap.isEmpty()) {
            validationReport.put("sampleMetadata", "sample metadata json is empty");
            validationMap.put("validationStatus", Boolean.FALSE);
            validationMap.put("validationReport", validationReport);
            return validationMap;
        }

        Boolean validationStatus = Boolean.TRUE;
        if (!hasBaitSet(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("baitSet", "missing");
        }
        if (!hasNormalizedPatientId(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("normalizedPatientId", "missing from 'cmoSampleIdFields'");
        }
        validationMap.put("validationStatus", validationStatus);
        validationMap.put("validationReport", validationReport);
        return validationMap;
    }

    @Override
    public Boolean isCmo(String json) throws JsonProcessingException {
        if (isBlank(json)) {
            return null;
        }
        Map<String, Object> jsonMap = mapper.readValue(json, Map.class);
        return isCmo(jsonMap);
    }

    private Boolean isCmo(Map<String, Object> jsonMap) throws JsonProcessingException {
        String isCMO = getIsCmo(jsonMap);
        if (isBlank(isCMO)) {
            return Boolean.FALSE;
        }
        return Boolean.valueOf(isCMO);
    }

    private String getIsCmo(Map<String, Object> jsonMap) throws JsonProcessingException {
        if (jsonMap.containsKey("isCmoRequest")) {
            return jsonMap.get("isCmoRequest").toString();
        }
        if (jsonMap.containsKey("additionalProperties")) {
            Map<String, String> additionalProperties = mapper.convertValue(
                    jsonMap.get("additionalProperties"), Map.class);
            if (additionalProperties.containsKey("isCmoSample")) {
                return additionalProperties.get("isCmoSample");
            }
        }
        return null;
    }

    @Override
    public String getRequestId(String json) throws JsonProcessingException {
        if (isBlank(json)) {
            return null;
        }
        Map<String, Object> jsonMap = mapper.readValue(json, Map.class);
        return getRequestId(jsonMap);
    }

    private String getRequestId(Map<String, Object> jsonMap) throws JsonProcessingException {
        if (jsonMap.containsKey("requestId")) {
            return jsonMap.get("requestId").toString();
        }
        if (jsonMap.containsKey("igoRequestId")) {
            return jsonMap.get("igoRequestId").toString();
        }
        if (jsonMap.containsKey("additionalProperties")) {
            Map<String, String> additionalProperties = mapper.convertValue(
                    jsonMap.get("additionalProperties"), Map.class);
            if (additionalProperties.containsKey("requestId")) {
                return additionalProperties.get("requestId");
            }
            if (additionalProperties.containsKey("igoRequestId")) {
                return additionalProperties.get("igoRequestId");
            }
        }
        return null;
    }

    private Boolean hasRequestId(Map<String, Object> jsonMap) throws JsonProcessingException {
        String requestId = getRequestId(jsonMap);
        return (!isBlank(requestId));
    }

    @Override
    public Boolean hasRequestId(String json) throws JsonProcessingException {
        String requestId = getRequestId(json);
        return (!isBlank(requestId));
    }

    private Boolean hasIgoId(Map<String, Object> sampleMap) {
        Object igoIdOrPrimaryId = ObjectUtils.firstNonNull(sampleMap.get("igoId"),
               sampleMap.get("primaryId"));
        if (igoIdOrPrimaryId == null) {
            return Boolean.FALSE;
        }
        return !isBlank(String.valueOf(igoIdOrPrimaryId));
    }

    private Boolean hasBaitSetOrRecipe(Map<String, Object> sampleMap) {
        return hasBaitSet(sampleMap) || hasRecipe(sampleMap);
    }

    private Boolean hasRecipe(Map<String, Object> sampleMap) {
        String recipe = null;
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdFields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            recipe = cmoSampleIdFields.get("recipe");
        }
        return !isBlank(recipe);
    }

    private Boolean hasBaitSet(Map<String, Object> sampleMap) {
        return !isBlank((String) sampleMap.get("baitSet"));
    }

    private Boolean hasInvestigatorSampleId(Map<String, Object> sampleMap) {
        return !isBlank((String) sampleMap.get("investigatorSampleId"));
    }

    private Boolean hasCmoPatientId(Map<String, Object> sampleMap) {
        return !isBlank((String) sampleMap.get("cmoPatientId"));
    }

    private Boolean hasFastQs(Map<String, Object> sampleMap) {
        // libraries -> runs -> fastqs [string list]
        if (!sampleMap.containsKey("libraries")) {
            return Boolean.FALSE;
        }
        List<Object> libraries = mapper.convertValue(sampleMap.get("libraries"), List.class);
        if (libraries.isEmpty()) {
            return Boolean.FALSE;
        }
        for (Object lib : libraries) {
            Map<String, Object> libMap = mapper.convertValue(lib, Map.class);
            if (!libMap.containsKey("runs")) {
                continue;
            }
            List<Object> runs = mapper.convertValue(libMap.get("runs"), List.class);
            if (runs.isEmpty()) {
                continue;
            }
            for (Object run : runs) {
                Map<String, Object> runMap = mapper.convertValue(run, Map.class);
                if (runMap.containsKey("fastqs")) {
                    String[] fastqs = mapper.convertValue(runMap.get("fastqs"),
                            String[].class);
                    if (fastqs.length > 0) {
                        return  Boolean.TRUE;
                    }
                }
            }
        }
        return Boolean.FALSE;
    }

    /**
     * Determines whether sample has a valid specimen type or has a valid alternative
     * to fall back on.
     *
     * <p>Option 1: specimen type is cellline, pdx, xenograft, xenograftderivedcelline, organoid
     * Option 2: if none from option 1 then fall back on sample origin
     *
     * @param sampleMap
     * @return
     */
    private Boolean hasValidSpecimenType(Map<String, Object> sampleMap) {
        //this can also be sampleClass
        Object specimenTypeObject = ObjectUtils.firstNonNull(sampleMap.get("specimenType"),
               sampleMap.get("sampleClass"));
        String specimenType = String.valueOf(specimenTypeObject);
        if (isBlank(specimenType)
                || !EnumUtils.isValidEnumIgnoreCase(SpecimenType.class, specimenType)) {
            return hasCmoSampleClass(sampleMap);
        }
        // check if specimen type is cellline, pdx, xenograft, xenograftderivedcellline, or organoid
        if (SpecimenType.CELLLINE.getValue().equalsIgnoreCase(specimenType)
                || SpecimenType.PDX.getValue().equalsIgnoreCase(specimenType)
                || SpecimenType.XENOGRAFT.getValue().equalsIgnoreCase(specimenType)
                || SpecimenType.XENOGRAFTDERIVEDCELLLINE.getValue().equalsIgnoreCase(specimenType)
                || SpecimenType.ORGANOID.getValue().equalsIgnoreCase(specimenType)) {
            return hasCmoSampleClass(sampleMap);
        }
        // if specimen type is none of the above then check if exosome or cfdna
        // and use sample origin if true
        if (SpecimenType.EXOSOME.getValue().equalsIgnoreCase(specimenType)
                || SpecimenType.CFDNA.getValue().equalsIgnoreCase(specimenType)) {
            return hasSampleOrigin(sampleMap);
        }
        return Boolean.TRUE;
    }

    private Boolean hasCmoSampleClass(Map<String, Object> sampleMap) {
        Object cmoSampleClassObject = ObjectUtils.firstNonNull(sampleMap.get("cmoSampleClass"),
                sampleMap.get("sampleType"));
        String cmoSampleClass = String.valueOf(cmoSampleClassObject);
        if (isBlank(cmoSampleClass)
                || !EnumUtils.isValidEnumIgnoreCase(CmoSampleClass.class,
                        cmoSampleClass)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean hasSampleOrigin(Map<String, Object> sampleMap) {
        if (isBlank((String) sampleMap.get("sampleOrigin"))
                || !EnumUtils.isValidEnumIgnoreCase(SampleOrigin.class,
                        (String) sampleMap.get("sampleOrigin"))) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * Determines whether sample type is valid.
     * - if sample type is null or empty then check na to extract
     * - if sampletype is pooled library then check recipe/baitset
     * - return true if sample type is a valid enum
     * @throws JsonProcessingException or JsonMappingException
     */
    private Boolean hasSampleType(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        String sampleType = null;
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdFields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            // relax the check on naToExtract and instead see if field is simply present
            // if naToExtract field is present but empty then the label generator assumes DNA
            sampleType = cmoSampleIdFields.get("sampleType");
        }

        return ((isBlank(sampleType) && hasNAtoExtract(sampleMap))
                || (SampleType.POOLED_LIBRARY.getValue().equalsIgnoreCase(sampleType)
                && hasBaitSet(sampleMap))
                || EnumUtils.isValidEnumIgnoreCase(SampleType.class, sampleType));
    }

    private Boolean hasNAtoExtract(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdFields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            // relax the check on naToExtract and instead see if field is simply present
            // if naToExtract field is present but empty then the label generator assumes DNA
            return cmoSampleIdFields.containsKey("naToExtract");
        }
        return Boolean.FALSE;
    }

    private Boolean hasNormalizedPatientId(Map<String, Object> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdfields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            return cmoSampleIdfields.containsKey("normalizedPatientId");
        }
        return Boolean.FALSE;
    }

    private Boolean requestHasSamples(String requestJson) throws JsonProcessingException, IOException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        if (!requestJsonMap.containsKey("samples")) {
            LOG.warn("Skipping request that is missing 'samples' in JSON");
            requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.REQUEST_WITH_MISSING_SAMPLES);
            return Boolean.FALSE;
        }

        // extract sample list from request json and check size is non-zero
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        if (sampleList.length == 0) {
            LOG.warn("Skipping request without any sample data in 'samples' JSON field");
            requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.REQUEST_WITH_MISSING_SAMPLES);
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean isBlank(String value) {
        return (Strings.isBlank(value) || value.equals("null"));
    }
}
