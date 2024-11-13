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
import org.mskcc.smile.commons.enums.CmoSampleClass;
import org.mskcc.smile.commons.enums.SampleOrigin;
import org.mskcc.smile.commons.enums.SampleType;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.service.ValidRequestChecker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ValidRequestCheckerImpl implements ValidRequestChecker {

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
     * @param requestJson
     * @return String
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
        List<Object> invalidRequestSamplesStatuses = new ArrayList<>();
        int validSampleCount = 0;
        for (Object sample: sampleList) {
            Map<String, Object> sampleMap = mapper.convertValue(sample, Map.class);
            Map<String, Object> sampleStatus;
            if (isCmoRequest) {
                sampleStatus = generateCmoSampleValidationMap(sampleMap);
            } else {
                sampleStatus = generateNonCmoSampleValidationMap(sampleMap);
            }
            sampleMap.put("status", sampleStatus);
            Object sampleObj = mapper.convertValue(sampleMap, Object.class);

            // get validation report and check status
            Map<String, String> validationReport =
                    mapper.convertValue(sampleStatus.get("validationReport"), Map.class);
            if ((Boolean) sampleStatus.get("validationStatus")) {
                validSampleCount++;
                updatedSampleList.add(sampleObj);
            } else {
                // do not add samples from cmo request if they are missing cmo patient ids
                if (validationReport.containsKey("cmoPatientId")) {
                    LOG.warn("Adding CMO sample with missing CMO patient ID to request-level "
                            + "validation report (failed samples): " + mapper.writeValueAsString(sampleMap));
                    invalidRequestSamplesStatuses.add(sampleObj);
                } else {
                    updatedSampleList.add(sampleObj);
                }
            }
        }
        // update request json with request status and samples containing validation reports
        if ((Boolean) requestStatus.get("validationStatus")) {
            Map<String, Object> requestValidationReport = new HashMap<>();
            // set validation status for request to false if none of the samples passed
            if (validSampleCount == 0) {
                requestStatus.replace("validationStatus", Boolean.FALSE);
            }
            // only replace contents of the validation report if the count of valid
            // samples is less than the original sample list
            if (validSampleCount < sampleList.length) {
                requestValidationReport.put("samples",
                        mapper.convertValue(invalidRequestSamplesStatuses, Object.class));
                requestStatus.replace("validationReport", requestValidationReport);
            }
        }
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
            String requestId = getRequestId(requestJson);
            LOG.warn("One or more sample(s) is missing one or a combination of the following: igoId or "
                    + "primaryId, cmoPatientId or cmoSampleIdFields --> normalizedPatientId - this "
                    + "information must be added for promoted requests & samples: requestId = "
                    + requestId + ", " + requestJson);
        }
        if (validPromotedSampleCount == 0) {
            requestStatus.put("validationStatus", Boolean.FALSE);
            Map<String, Object> requestValidationReport =
                    (Map<String, Object>) requestStatus.get("validationReport");
            requestValidationReport.put("samples", "All samples in the promoted "
                    + "IGO request JSON failed validation.");
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
        validationMap.put("validationReport", mapper.writeValueAsString(validationReport));
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
        Map<String, Object> validationReport = new HashMap<>();
        if (StringUtils.isAllBlank(requestJson)) {
            validationReport.put("requestJson", "Request JSON received is empty");
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
            LOG.warn("CMO request failed sanity checking - missing requestId. " + requestJson);
            validationReport.put("requestId", "IGO Request ID is missing from the request JSON received.");
            validationStatus = Boolean.FALSE;
        }

        // if cmo filter is enabled then skip request if it is non-cmo
        if (igoCmoRequestFilter && !isCmoRequest) {
            LOG.warn("CMO request filter enabled - skipping non-CMO request: "
                    + getRequestId(requestJsonMap) + ", " + requestJson);
            validationReport.put("isCmo", "SMILE CMO request filter is enabled and request JSON received has"
                    + " 'cmoRequest': false. This value must be set to true for import into SMILE.");
            validationStatus = Boolean.FALSE;
        }

        // determine whether request json has samples
        if (!requestHasSamples(requestJson)) {
            validationReport.put("samples", "Request JSON is missing 'samples' or "
                    + "'samples' is an empty list.");
            validationStatus = Boolean.FALSE;
        }

        // update contents of validation map to return
        validationMap.put("validationStatus", validationStatus);
        validationMap.put("validationReport", mapper.writeValueAsString(validationReport));
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
        if (!isIgoComplete(sampleMap)) {
            validationStatus = Boolean.FALSE;
            validationReport.put("igoComplete", "false");
        }
        validationMap.put("validationStatus", validationStatus);
        validationMap.put("validationReport", mapper.writeValueAsString(validationReport));
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
        validationMap.put("validationReport", mapper.writeValueAsString(validationReport));
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

    private String getPrimaryOrIgoId(Map<String, Object> sampleMap) {
        Object igoIdOrPrimaryId = ObjectUtils.firstNonNull(sampleMap.get("igoId"),
               sampleMap.get("primaryId"));
        return isBlank(String.valueOf(igoIdOrPrimaryId)) ? null : String.valueOf(igoIdOrPrimaryId);
    }

    private Boolean hasIgoId(Map<String, Object> sampleMap) {
        Object igoIdOrPrimaryId = getPrimaryOrIgoId(sampleMap);
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

    private Boolean isIgoComplete(Map<String, Object> sampleMap) {
        return (Boolean) sampleMap.get("igoComplete");
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
        String requestId = getRequestId(requestJson);
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        if (!requestJsonMap.containsKey("samples")) {
            LOG.warn("Skipping request that is missing 'samples' in JSON for request ID: "
                    + requestId + ", " + requestJson);
            return Boolean.FALSE;
        }

        // extract sample list from request json and check size is non-zero
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        if (sampleList.length == 0) {
            LOG.warn("Skipping request without any sample data in 'samples' JSON field: "
                    + requestId + ", " + requestJson);
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean isBlank(String value) {
        return (StringUtils.isBlank(value) || value.equals("null"));
    }

    @Override
    public String generateValidationReport(String originalJson, String filteredJson)
            throws JsonProcessingException {
        StringBuilder builder = new StringBuilder();
        String requestId = getRequestId(originalJson);
        Map<String, Object> filteredJsonMap = mapper.readValue(filteredJson, Map.class);
        // keeps track if there's anything to report or not. if still true after all checks
        // then return null
        Boolean allValid = Boolean.TRUE;

        // if request-level status is missing from the filtered json then
        // a critical error likely occurred, in which case the original json
        // would be more helpful to have as a reference when debugging the error
        if (!filteredJsonMap.containsKey("status")) {
            allValid = Boolean.FALSE;
            builder.append("[request-filter] Request JSON missing validation report ('status') ");
            builder.append(" post-validation: Original JSON contents: ")
                    .append(originalJson).append("Filtered JSON contents: ")
                    .append(filteredJson);
        } else {
            Map<String, Object> statusMap = (Map<String, Object>) filteredJsonMap.get("status");
            Map<String, Object> validationReport =
                    mapper.convertValue(statusMap.get("validationReport"), Map.class);

            // if request validation report is not empty then log for ddog
            if (!validationReport.isEmpty()) {
                allValid = Boolean.FALSE;
                builder.append("[request-filter] Request-level status and validation report for request '")
                        .append(requestId)
                        .append("': ")
                        .append(mapper.writeValueAsString(statusMap));
            }
            // check validation status for each sample individually as well and
            // add contents to report for ddog
            Object[] sampleList = mapper.convertValue(filteredJsonMap.get("samples"),
                Object[].class);
            for (Object s : sampleList) {
                Map<String, Object> sampleMap = mapper.convertValue(s, Map.class);
                Map<String, Object> sampleStatusMap
                        = mapper.convertValue(sampleMap.get("status"), Map.class);
                Map<String, String> sampleValidationReport =
                        mapper.convertValue(sampleStatusMap.get("validationReport"), Map.class);
                try {
                    String sampleId = ObjectUtils.firstNonNull(
                            sampleMap.get("igoId"), sampleMap.get("primaryId")).toString();
                    if (!sampleValidationReport.isEmpty()) {
                        allValid = Boolean.FALSE;
                        builder.append("\n[request-filter] Validation report for sample '")
                                .append(sampleId)
                                .append("': ")
                                .append(mapper.writeValueAsString(sampleStatusMap));
                    }
                } catch (NullPointerException e) {
                    builder.append("\n[request-filter] No known identifiers in current sample data: ")
                            .append(mapper.writeValueAsString(sampleMap))
                            .append(", Validation report for unknown sample: ")
                            .append(mapper.writeValueAsString(sampleStatusMap));
                }
            }
        }
        // if allValid is still true then there wasn't anything to report at the request
        // or sample level.. return null
        return allValid ? null : builder.toString();
    }
}
