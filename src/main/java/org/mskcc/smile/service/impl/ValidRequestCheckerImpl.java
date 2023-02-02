package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
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
        // first check if request-level metadata is valid
        if (!hasValidRequestLevelMetadata(requestJson)) {
            return null;
        }

        // verify that request has 'samples' json field
        if (!requestHasSamples(requestJson)) {
            return null;
        }

        // extract valid samples from request json
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);

        // validate each sample json and add to validSampleList if it passes check
        Boolean isCmoRequest = isCmo(requestJsonMap);
        List<Object> validSampleList = new ArrayList<Object>();
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            if (isCmoRequest && isValidCmoSample(sampleMap)) {
                validSampleList.add(sample);
            } else if (!isCmoRequest && isValidNonCmoSample(sampleMap)) {
                validSampleList.add(sample);
            }
        }

        // update 'samples' field in json with valid samples list
        // if valid samples total does not match the original samples total then
        // log request with request status logger but allow request to be published
        if (validSampleList.size() > 0) {
            if (validSampleList.size() < sampleList.length) {
                requestJsonMap.replace("samples", validSampleList.toArray(new Object[0]));
                LOG.info("CMO request passed sanity checking with some samples missing "
                        + "required CMO label fields");
                requestStatusLogger.logRequestStatus(requestJson,
                        RequestStatusLogger.StatusType.CMO_REQUEST_WITH_SAMPLES_MISSING_CMO_LABEL_FIELDS);
            }
            return mapper.writeValueAsString(requestJsonMap);
        }
        LOG.error("Request failed sanity checking - logging request status...");
        requestStatusLogger.logRequestStatus(requestJson,
                RequestStatusLogger.StatusType.CMO_REQUEST_FAILED_SANITY_CHECK);
        return null;
    }

    @Override
    public Boolean isValidPromotedRequest(String requestJson) throws JsonMappingException,
            JsonProcessingException, IOException {
        // first check if request-level metadata is valid
        if (!hasValidRequestLevelMetadata(requestJson)) {
            return Boolean.FALSE;
        }

        // verify that request has 'samples' json field
        if (!requestHasSamples(requestJson)) {
            return Boolean.FALSE;
        }

        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            // sample should have at least an igoId/primaryId and at least a cmoPatientId/normalizedPatientId
            if (!hasIgoId(sampleMap) || !(hasCmoPatientId(sampleMap) || hasNormalizedPatientId(sampleMap))) {
                LOG.warn("Sample is missing one or a combination of the following: igoId or primaryId, "
                        + "cmoPatientId or cmoSampleIdFields --> normalizedPatientId - this information "
                        + "must be added for promoted requests & samples");
                requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.PROMOTED_SAMPLES_MISSING_IDS);
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    /**
     * Evaluates request metadata and returns a boolean based on whether the request data
     * passes all sanity checks.
     * @throws IOException
     */
    @Override
    public Boolean hasValidRequestLevelMetadata(String requestJson)
            throws IOException {
        if (StringUtils.isAllBlank(requestJson)) {
            return Boolean.FALSE;
        }
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        boolean isCmoRequest = isCmo(requestJsonMap);
        boolean hasRequestId = hasRequestId(requestJsonMap);

        // if requestId is blank then nothing to do, return null
        if (!hasRequestId) {
            LOG.warn("CMO request failed sanity checking - missing requestId...");
            requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.CMO_REQUEST_WITH_SAMPLES_MISSING_CMO_LABEL_FIELDS);
            return Boolean.FALSE;
        }

        // if cmo filter is enabled then skip request if it is non-cmo
        if (igoCmoRequestFilter && !isCmoRequest) {
            LOG.warn("CMO request filter enabled - skipping non-CMO request: "
                    + getRequestId(requestJsonMap));
            requestStatusLogger.logRequestStatus(requestJson,
                    RequestStatusLogger.StatusType.CMO_REQUEST_FILTER_SKIPPED_REQUEST);
            return Boolean.FALSE;
        }

        return Boolean.TRUE;
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
     * @throws JsonProcessingException or JsonMappingException
     */
    @Override
    public Boolean isValidCmoSample(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        if (sampleMap == null || sampleMap.isEmpty()) {
            return Boolean.FALSE;
        }
        if (!hasInvestigatorSampleId(sampleMap)
                || !hasIgoId(sampleMap)
                || !hasBaitSetOrRecipe(sampleMap)
                || !hasCmoPatientId(sampleMap)
                || !(hasValidSpecimenType(sampleMap) || hasSampleType(sampleMap))
                || !hasNormalizedPatientId(sampleMap) || !hasFastQs(sampleMap)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    /**
     * Evaluates sample metadata for samples from NON-CMO requests.
     * - Checks if sample map has all required fields.
     * - If bait set or normalized patient id are  missing then returns false.
     * @throws JsonProcessingException or JsonMappingException
     */
    @Override
    public Boolean isValidNonCmoSample(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        if (sampleMap == null
                || sampleMap.isEmpty()
                || !hasBaitSet(sampleMap)
                || !hasNormalizedPatientId(sampleMap)) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
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

    private Boolean hasIgoId(Map<String, String> sampleMap) {
        Object igoIdOrPrimaryId = ObjectUtils.firstNonNull(sampleMap.get("igoId"),
               sampleMap.get("primaryId"));
        if (igoIdOrPrimaryId == null) {
            return Boolean.FALSE;
        }
        return !isBlank(String.valueOf(igoIdOrPrimaryId));
    }

    private Boolean hasBaitSetOrRecipe(Map<String, String> sampleMap) {
        return hasBaitSet(sampleMap) || hasRecipe(sampleMap);
    }

    private Boolean hasRecipe(Map<String, String> sampleMap) {
        String recipe = null;
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdFields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            recipe = cmoSampleIdFields.get("recipe");
        }
        return !isBlank(recipe);
    }

    private Boolean hasBaitSet(Map<String, String> sampleMap) {
        return !isBlank(sampleMap.get("baitSet"));
    }

    private Boolean hasInvestigatorSampleId(Map<String, String> sampleMap) {
        return !isBlank(sampleMap.get("investigatorSampleId"));
    }

    private Boolean hasCmoPatientId(Map<String, String> sampleMap) {
        return !isBlank(sampleMap.get("cmoPatientId"));
    }

    private Boolean hasFastQs(Map<String, String> sampleMap) {
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
    private Boolean hasValidSpecimenType(Map<String, String> sampleMap) {
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

    private Boolean hasCmoSampleClass(Map<String, String> sampleMap) {
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

    private Boolean hasSampleOrigin(Map<String, String> sampleMap) {
        if (isBlank(sampleMap.get("sampleOrigin"))
                || !EnumUtils.isValidEnumIgnoreCase(SampleOrigin.class, sampleMap.get("sampleOrigin"))) {
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
    private Boolean hasSampleType(Map<String, String> sampleMap)
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

    private Boolean hasNAtoExtract(Map<String, String> sampleMap)
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

    private Boolean hasNormalizedPatientId(Map<String, String> sampleMap)
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
