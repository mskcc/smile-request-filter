package org.mskcc.smile.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.mskcc.smile.commons.enums.CmoSampleClass;
import org.mskcc.smile.commons.enums.SampleOrigin;
import org.mskcc.smile.commons.enums.SampleType;
import org.mskcc.smile.commons.enums.SpecimenType;
import org.mskcc.smile.model.Status;
import org.mskcc.smile.service.ValidRequestChecker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ValidRequestCheckerImpl implements ValidRequestChecker {
    @Value("${igo.cmo_request_filter:false}")
    private Boolean igoCmoRequestFilter;

    private final ObjectMapper mapper = new ObjectMapper();
    
    List<Map.Entry<String, ErrorDesc>> requestValidationReport;
    List<Map.Entry<String, ErrorDesc>> sampleValidationReport;
    
    public enum ErrorDesc {
        NULL_OR_EMPTY,
        INVALID_TYPE,
        PARSING_ERROR,
        SKIPPED_NON_CMO_REQUEST,
    }

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
        requestValidationReport = new ArrayList<Map.Entry<String, ErrorDesc>>();
        if (StringUtils.isAllBlank(requestJson)) {
            // This should be dropped
            return null;
        }
        
        Map<String, String> requestJsonMapString = mapper.readValue(requestJson, Map.class);
        
        // first check if request-level metadata is valid
        if (!hasValidRequestLevelMetadata(requestJson)) {
            return addStatusToMetadataJson(requestJsonMapString, Boolean.FALSE, requestValidationReport);
        }

        // verify that request has 'samples' json field
        if (!requestHasSamples(requestJson)) {
            addEntryToValidationReport(requestValidationReport, "samples", ErrorDesc.NULL_OR_EMPTY);
            return addStatusToMetadataJson(requestJsonMapString, Boolean.FALSE, requestValidationReport);
        }

        // extract valid samples from request json
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);

        // validate each sample json and add to validSampleList if it passes check
        Boolean isCmoRequest = isCmo(requestJsonMap);
        List<Object> sampleListWithStatus = new ArrayList<Object>();
        for (Object sample: sampleList) {
            sampleValidationReport = new ArrayList<Map.Entry<String, ErrorDesc>>();
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            // If sample json is null/empty or missing primaryId or investigatorId,
            // sample json will be dropped without logging
            if (sampleMap != null && !sampleMap.isEmpty()
                    && hasIgoId(sampleMap) && hasInvestigatorSampleId(sampleMap)) {
                if ((isCmoRequest && isValidCmoSample(sampleMap))
                        ||(!isCmoRequest && isValidNonCmoSample(sampleMap))) {
                    sampleListWithStatus.add(addStatusToMetadataJson(sampleMap,
                            Boolean.TRUE, sampleValidationReport));
                }
                sampleListWithStatus.add(addStatusToMetadataJson(sampleMap,
                        Boolean.FALSE, sampleValidationReport));
            }
        }

        if (sampleListWithStatus.size() > 0) {
            requestJsonMap.replace("samples", mapper.writeValueAsString(sampleListWithStatus));
            return addStatusToMetadataJson(requestJsonMapString,
                    Boolean.TRUE, requestValidationReport);
        }
        // request json contains null/empty samples
        addEntryToValidationReport(requestValidationReport, "samples", ErrorDesc.NULL_OR_EMPTY);
        return addStatusToMetadataJson(requestJsonMapString, Boolean.FALSE, requestValidationReport);
    }
    
    @Override
    public String validateAndUpdateRequestMetadata(String requestJson) throws IOException {
        requestValidationReport = new ArrayList<Map.Entry<String, ErrorDesc>>();
        Map<String, String> requestJsonMap = mapper.readValue(requestJson, Map.class);
        if (hasValidRequestLevelMetadata(requestJson)) {
            return addStatusToMetadataJson(requestJsonMap, Boolean.TRUE, requestValidationReport);
        }
        return addStatusToMetadataJson(requestJsonMap, Boolean.FALSE, requestValidationReport);
    }
    
    @Override
    public String validateAndUpdateSampleMetadata(String sampleJson, Boolean isCmo) throws JsonProcessingException {
        sampleValidationReport = new ArrayList<Map.Entry<String, ErrorDesc>>();
        Map<String, String> sampleMap = mapper.readValue(sampleJson, Map.class);
        if (StringUtils.isEmpty(sampleJson)) {
            return null;
        }
        if (!hasRequestId(sampleJson)) {
            // CASE 1: missing requestId
            addEntryToValidationReport(sampleValidationReport, "requestId", ErrorDesc.NULL_OR_EMPTY);
            return addStatusToMetadataJson(sampleMap, Boolean.FALSE, sampleValidationReport);
        }
        if (isCmo) {
            // CASE 2: CMO sample
            if (isValidCmoSample(sampleMap)) {
                // CASE 2a: CMO sample passed validation
                return addStatusToMetadataJson(sampleMap, Boolean.TRUE, sampleValidationReport);
            }
            // CASE 2b: CMO sample failed validation
            return addStatusToMetadataJson(sampleMap, Boolean.FALSE, sampleValidationReport);
        } else {
            // CASE 3: Non-CMO sample
            if (isValidNonCmoSample(sampleMap)) {
                // CASE 3a: Non-CMO sample passed validation
                return addStatusToMetadataJson(sampleMap, Boolean.TRUE, sampleValidationReport);
            }
            // CASE 3b: Non-CMO sample failed validation
            return addStatusToMetadataJson(sampleMap, Boolean.FALSE, sampleValidationReport);
        }
    }

    @Override
    public Boolean isValidPromotedRequest(String requestJson) throws JsonMappingException,
            JsonProcessingException, IOException {
        if (StringUtils.isAllBlank(requestJson)) {
            // This should be dropped
            return Boolean.FALSE;
        }
        // first check if request-level metadata is valid
        if (!hasValidRequestLevelMetadata(requestJson)) {
            // return added status json
            return Boolean.FALSE;
        }

        // verify that request has 'samples' json field
        if (!requestHasSamples(requestJson)) {
            // return added status field
            return Boolean.FALSE;
        }

        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        for (Object sample: sampleList) {
            Map<String, String> sampleMap = mapper.convertValue(sample, Map.class);
            // sample should have at least an igoId/primaryId and at least a cmoPatientId/normalizedPatientId
            if (!hasIgoId(sampleMap) || !(hasCmoPatientId(sampleMap) || hasNormalizedPatientId(sampleMap))) {
                // add missing fields to validationReport
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
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        boolean isCmoRequest = isCmo(requestJsonMap);
        boolean hasRequestId = hasRequestId(requestJsonMap);

        // if requestId is blank then nothing to do, return null
        if (!hasRequestId) {
            requestValidationReport.add(new AbstractMap.SimpleImmutableEntry<>(
                    "igoRequestId",ErrorDesc.NULL_OR_EMPTY));
            return Boolean.FALSE;
        }

        // if cmo filter is enabled then skip request if it is non-cmo
        if (igoCmoRequestFilter && !isCmoRequest) {
            requestValidationReport.add(new AbstractMap.SimpleImmutableEntry<>(
                    "isCmoRequest",ErrorDesc.SKIPPED_NON_CMO_REQUEST));
            return Boolean.FALSE;
        }

        return Boolean.TRUE;
    }
    
    String addStatusToMetadataJson(Map<String, String> jsonMap, Boolean isValid, List<Map.Entry<String, ErrorDesc>> validationReport) throws JsonMappingException, JsonProcessingException {
        // Map<String, String> jsonMap = mapper.readValue(json, Map.class);
        Status status = new Status(isValid, validationReport.toString());
        jsonMap.put("status", mapper.writeValueAsString(status));
        //System.out.println("\n\n\n\nSTATUS\n\n\n\n" + jsonMap.get("status"));

        return mapper.writeValueAsString(jsonMap);
    }
    
    void addEntryToValidationReport(List<Map.Entry<String, ErrorDesc>> validationReport,
            String fieldName, ErrorDesc desc) {
        System.out.println("\n\n\n\nADDING NEW REPORT\n\n\n\n" + fieldName + desc);
        validationReport.add(new AbstractMap.SimpleImmutableEntry<>(fieldName, desc));
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
        if (hasInvestigatorSampleId(sampleMap)
                && hasIgoId(sampleMap)
                && hasBaitSetOrRecipe(sampleMap)
                && hasCmoPatientId(sampleMap)
                && (hasValidSpecimenType(sampleMap) || hasSampleType(sampleMap))
                && hasNormalizedPatientId(sampleMap) 
                && hasFastQs(sampleMap)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
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
        if (hasBaitSet(sampleMap)
                && hasNormalizedPatientId(sampleMap)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
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
            if (!isBlank(cmoSampleIdFields.get("recipe"))) {
                return Boolean.TRUE;
            }
        }
        addEntryToValidationReport(sampleValidationReport, "recipe",
                ErrorDesc.NULL_OR_EMPTY);
        return Boolean.FALSE;
    }

    private Boolean hasBaitSet(Map<String, String> sampleMap) {
        if (!isBlank(sampleMap.get("baitSet"))) {
            addEntryToValidationReport(sampleValidationReport, "baitSet",
                    ErrorDesc.NULL_OR_EMPTY);
            System.out.println("\n\n\n\n\nEMPTY baitset");
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean hasInvestigatorSampleId(Map<String, String> sampleMap) {
        return !isBlank(sampleMap.get("investigatorSampleId"));
    }

    private Boolean hasCmoPatientId(Map<String, String> sampleMap) {
        if (!isBlank(sampleMap.get("cmoPatientId"))) {
            return Boolean.TRUE;
        }
        addEntryToValidationReport(sampleValidationReport, "cmoPatientId",
                ErrorDesc.NULL_OR_EMPTY);
        return Boolean.FALSE;
    }

    private Boolean hasFastQs(Map<String, String> sampleMap) {
        // libraries -> runs -> fastqs [string list]
        if (!sampleMap.containsKey("libraries")) {
            addEntryToValidationReport(sampleValidationReport, "libraries",
                    ErrorDesc.NULL_OR_EMPTY);
            return Boolean.FALSE;
        }
        List<Object> libraries = mapper.convertValue(sampleMap.get("libraries"), List.class);
        if (libraries.isEmpty()) {
            addEntryToValidationReport(sampleValidationReport, "libraries",
                    ErrorDesc.NULL_OR_EMPTY);
            return Boolean.FALSE;
        }
        for (Object lib : libraries) {
            Map<String, Object> libMap = mapper.convertValue(lib, Map.class);
            if (!libMap.containsKey("runs")) {
                addEntryToValidationReport(sampleValidationReport, "runs",
                        ErrorDesc.NULL_OR_EMPTY);
                return Boolean.FALSE;
            }
            List<Object> runs = mapper.convertValue(libMap.get("runs"), List.class);
            if (runs.isEmpty()) {
                addEntryToValidationReport(sampleValidationReport, "runs",
                        ErrorDesc.NULL_OR_EMPTY);
                return Boolean.FALSE;
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
        addEntryToValidationReport(sampleValidationReport, "fastQs",
                ErrorDesc.NULL_OR_EMPTY);
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
        if (isBlank(cmoSampleClass)) {
            addEntryToValidationReport(sampleValidationReport, "cmoSampleClass",
                    ErrorDesc.NULL_OR_EMPTY);
            return Boolean.FALSE;
        }
        if (!EnumUtils.isValidEnumIgnoreCase(CmoSampleClass.class, cmoSampleClass)) {
            addEntryToValidationReport(sampleValidationReport, "cmoSampleClass",
                    ErrorDesc.INVALID_TYPE);
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean hasSampleOrigin(Map<String, String> sampleMap) {
        if (isBlank(sampleMap.get("sampleOrigin"))) {
            addEntryToValidationReport(sampleValidationReport, "sampleOrigin",
                    ErrorDesc.NULL_OR_EMPTY);
            return Boolean.FALSE;

        }
        if (!EnumUtils.isValidEnumIgnoreCase(SampleOrigin.class, sampleMap.get("sampleOrigin"))) {
            addEntryToValidationReport(sampleValidationReport, "sampleOrigin",
                    ErrorDesc.INVALID_TYPE);
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

        if ((isBlank(sampleType) && hasNAtoExtract(sampleMap))
                || (SampleType.POOLED_LIBRARY.getValue().equalsIgnoreCase(sampleType)
                && hasBaitSet(sampleMap))
                || EnumUtils.isValidEnumIgnoreCase(SampleType.class, sampleType)) {
            return Boolean.TRUE;
        }
        addEntryToValidationReport(sampleValidationReport, "sampleType",
                ErrorDesc.INVALID_TYPE);
        return Boolean.FALSE;
    }

    private Boolean hasNAtoExtract(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdFields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            // relax the check on naToExtract and instead see if field is simply present
            // if naToExtract field is present but empty then the label generator assumes DNA
            if (cmoSampleIdFields.containsKey("naToExtract")) {
                return Boolean.TRUE;
            }
        }
        addEntryToValidationReport(sampleValidationReport, "naToExtract",
                ErrorDesc.NULL_OR_EMPTY);
        return Boolean.FALSE;
    }

    private Boolean hasNormalizedPatientId(Map<String, String> sampleMap)
            throws JsonMappingException, JsonProcessingException {
        if (sampleMap.containsKey("cmoSampleIdFields")) {
            Map<String, String> cmoSampleIdfields = mapper.convertValue(
                    sampleMap.get("cmoSampleIdFields"), Map.class);
            if (cmoSampleIdfields.containsKey("normalizedPatientId")) {
                return Boolean.TRUE;
            }
        }
        addEntryToValidationReport(sampleValidationReport, "normalizedPatientId",
                ErrorDesc.NULL_OR_EMPTY);
        return Boolean.FALSE;
    }

    private Boolean requestHasSamples(String requestJson) throws JsonProcessingException, IOException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        if (!requestJsonMap.containsKey("samples")) {
            return Boolean.FALSE;
        }

        // extract sample list from request json and check size is non-zero
        Object[] sampleList = mapper.convertValue(requestJsonMap.get("samples"),
                Object[].class);
        if (sampleList.length == 0) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private Boolean isBlank(String value) {
        return (Strings.isBlank(value) || value.equals("null"));
    }
}
