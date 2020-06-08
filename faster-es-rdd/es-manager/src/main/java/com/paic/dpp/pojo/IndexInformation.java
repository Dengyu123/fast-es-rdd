package com.paic.dpp.pojo;

import com.alibaba.fastjson.JSONObject;
import com.paic.dpp.constant.XContentType;
import com.paic.dpp.util.HdfsUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/03
 */
public class IndexInformation implements Serializable {
    private JSONObject settings;
    private JSONObject mappings;
    private List<FieldInfo> fieldInfoList;
    private String indexName;
    private String type;
    private Set<String> nFields; //开启NESTED的FIELDS
    private boolean openNested; //是否开启NESTED

    public IndexInformation(String indexName, String type) {
        this.indexName = indexName;
        this.type = type;
    }
    public IndexInformation(String indexName) {
        this.indexName = indexName;
        this.type = "doc";
    }

    public boolean isOpenNested() {
        return openNested;
    }

    public void setOpenNested(boolean openNested) {
        this.openNested = openNested;
    }

    public JSONObject getSettings() {
        return settings;
    }

    public void setSettings(JSONObject settings) {
        this.settings = settings;
    }

    public JSONObject getMappings() {
        return mappings;
    }

    public void setMappings(JSONObject mappings) {
        this.mappings = mappings;
    }

    public List<FieldInfo> getFieldInfoList() {
        return fieldInfoList;
    }

    public void setFieldInfoList(List<FieldInfo> fieldInfoList) {
        this.fieldInfoList = fieldInfoList;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Set<String> getnFields() {
        return nFields;
    }

    public void setnFields(Set<String> nFields) {
        this.nFields = nFields;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * 从INDEX文件加载
     * @param indexPath 路径
     * @param indexName 索引名称
     * @param indexType 类型
     * @param openNested 是否打开NESTED
     * @param openDateDetect 是否打开日期检测
     * @return
     * @throws IOException
     */
    public static IndexInformation getIndexInfoFromIndexFile(String indexPath, String indexName, String indexType, boolean openNested, boolean openDateDetect, Configuration configuration) throws IOException {
        String file = HdfsUtil.download2LocalWithoutPrefix(indexPath,configuration);
        List<String> lines = FileUtils.readLines(new File(file));
        //nested
        Map<String,List<FieldInfo>> nestedInfoMap = new HashMap<>();
        Set<String> nfields = new HashSet<>();
        for (String line : lines) {
            String[] fieldInfoArray = line.split(",",-1);
            String hasNestedMark = fieldInfoArray[0];
            String fieldName = fieldInfoArray[1];
            String fieldType = fieldInfoArray[2];
            int participle = Integer.parseInt(fieldInfoArray[3]);
            if(fieldInfoArray.length != 4) continue;
            if(!"0".equals(hasNestedMark)){
                if(nestedInfoMap.containsKey(hasNestedMark)){
                    FieldInfo nfieldInfo = new FieldInfo(fieldName,fieldType,participle);
                    nestedInfoMap.get(hasNestedMark).add(nfieldInfo);
                }else{
                    List<FieldInfo> nfieldInfoList = new ArrayList<>();
                    FieldInfo nfieldInfo = new FieldInfo(fieldName,fieldType,participle);
                    nfieldInfoList.add(nfieldInfo);
                    nestedInfoMap.put(hasNestedMark,nfieldInfoList);
                }
            }else{
                continue;
            }

        }
        //not nested
        List<FieldInfo> fieldInfoList = new ArrayList<>();
        for (String line : lines) {
            String[] fieldInfoArray = line.split(",", -1);
            String hasNestedMark = fieldInfoArray[0];
            String fieldName = fieldInfoArray[1];
            String fieldType = fieldInfoArray[2];
            int participle = Integer.parseInt(fieldInfoArray[3]);
            if (fieldInfoArray.length != 4) continue;
            if("0".equals(hasNestedMark)){
                FieldInfo fieldInfo = new FieldInfo(fieldName,fieldType,participle);
                if(XContentType.NESTED.equals(fieldType)) {
                    nfields.add(fieldName);
                    fieldInfo.setNestedFields(nestedInfoMap.get(fieldName));
                }
                fieldInfoList.add(fieldInfo);
            }else
                continue;
        }
        //build index information
        IndexInformation indexInformation = new IndexInformation(indexName.trim().toLowerCase());
        indexInformation.setFieldInfoList(fieldInfoList);
        indexInformation.setnFields(nfields);
        indexInformation.setOpenNested(openNested);
        indexInformation.setType("doc");
        JSONObject mapping = getMappingInit(openDateDetect);
        indexInformation.setMappings(mapping);
        JSONObject setting = getSettingInit();
        indexInformation.setSettings(setting);
        return indexInformation;
    }

    private static JSONObject getSettingInit() {
        return new JSONObject();
    }

    private static JSONObject getMappingInit(boolean openDateDetect) {
        JSONObject mapping = new JSONObject();
        mapping.put("date_detection",openDateDetect);
        return mapping;
    }

    public static IndexInformation getIndexInfoFromIndexFile(String indexPath,String indexName,boolean openNested,boolean openDateDetect,Configuration configuration) throws IOException {
        return getIndexInfoFromIndexFile(indexPath,indexName,"doc",openNested,openDateDetect,configuration);
    }
}
