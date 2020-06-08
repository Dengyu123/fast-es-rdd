package com.paic.dpp.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/03
 */
public class FieldInfo implements Serializable {
    private String fieldName;
    private String fieldType;
    private int participle;
    private List<FieldInfo> nestedFields;

    public FieldInfo(String fieldName, String fieldType, int participle) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.participle = participle;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public int getParticiple() {
        return participle;
    }

    public void setParticiple(int participle) {
        this.participle = participle;
    }

    public List<FieldInfo> getNestedFields() {
        return nestedFields;
    }

    public void setNestedFields(List<FieldInfo> nestedFields) {
        this.nestedFields = nestedFields;
    }

    @Override
    public String toString() {
        return "FieldInfo{" +
                "fieldName='" + fieldName + '\'' +
                ", fieldType='" + fieldType + '\'' +
                ", participle=" + participle +
                ", nestedFields=" + nestedFields +
                '}';
    }
}
