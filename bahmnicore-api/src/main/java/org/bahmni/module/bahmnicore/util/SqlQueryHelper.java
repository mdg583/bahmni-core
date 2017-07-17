package org.bahmni.module.bahmnicore.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bahmni.module.bahmnicore.model.searchParams.AdditionalSearchParam;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.lang.StringBuilder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlQueryHelper {
    private final Pattern paramPlaceHolderPattern;
    private static final String PARAM_PLACE_HOLDER_REGEX = "\\$\\{[^{]*\\}";
    private Log log = LogFactory.getLog(SqlQueryHelper.class);

    public SqlQueryHelper() {
        this.paramPlaceHolderPattern = Pattern.compile(PARAM_PLACE_HOLDER_REGEX);
    }

    List<String> getParamNamesFromPlaceHolders(String query){
        List<String> params  = new ArrayList<>();
        Matcher matcher = paramPlaceHolderPattern.matcher(query);
        while(matcher.find()){
            params.add(stripDelimiters(matcher.group()));
        }
        return params;
    }

    private String stripDelimiters(String text) {
        return text.replaceAll("[${}]", "");
    }

    public String transformIntoPreparedStatementFormat(String queryString){
        return  queryString.replaceAll(PARAM_PLACE_HOLDER_REGEX,"?");
    }

    private String getReplaceByParam(String[] param){
        if(param.length == 1) return "?";
        if(param.length > 1) {
            StringBuilder ret = new StringBuilder(1+2*param.length);
            ret.append('(');
            for(int i = 0; i < param.length-1; i++){
                ret.append("?,");
            }
            ret.append("?)");
            return ret.toString();
        }
        return "";
    }

    private String expandPreparedStatementParameters(String statement, List<String> paramNamesFromPlaceHolders, Map<String, String[]> params) {
        String ret = "";
        int i = 0;
        int n = 0;
        int j = statement.indexOf('?', i);
        while(j != -1){
            String[] param = params.get(paramNamesFromPlaceHolders.get(n++));
            String expanded = getReplaceByParam(param);
            ret += statement.substring(i, j) + expanded;
            i = j + 1;
            j = statement.indexOf('?', i);
        }
        ret += statement.substring(i);
        return ret;
    }

    public PreparedStatement constructPreparedStatement(String queryString, Map<String, String[]> params, Connection conn) throws SQLException {
       String finalQueryString = queryString;
        if (params.get("additionalParams") != null && params.get("additionalParams") != null) {
            finalQueryString = parseAdditionalParams(params.get("additionalParams")[0], queryString);
        }

        List<String> paramNamesFromPlaceHolders = getParamNamesFromPlaceHolders(finalQueryString);
        String statement = transformIntoPreparedStatementFormat(finalQueryString);
        statement = expandPreparedStatementParameters(statement, paramNamesFromPlaceHolders, params);
        PreparedStatement preparedStatement = conn.prepareStatement(statement);

        int i = 1;
        for (String paramName : paramNamesFromPlaceHolders) {
            if(params.get(paramName) != null) {
                for (String paramValue : params.get(paramName)) {
                    preparedStatement.setObject(i++, paramValue);
                }
            }
        }

        return preparedStatement;
    }

    String parseAdditionalParams(String additionalParams, String queryString) {
        String queryWithAdditionalParams = queryString;
        try {
            AdditionalSearchParam additionalSearchParams = new ObjectMapper().readValue(additionalParams, AdditionalSearchParam.class);
            String test = additionalSearchParams.getTests();
            queryWithAdditionalParams = queryString.replaceAll("\\$\\{testName\\}", test);
        } catch (IOException e) {
            log.error("Failed to parse Additional Search Parameters.");
            e.printStackTrace();
        }
        return queryWithAdditionalParams;
    }


}
