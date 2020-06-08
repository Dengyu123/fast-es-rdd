package com.paic.dpp.fastsync;

import com.alibaba.fastjson.JSONObject;
import com.paic.dpp.constant.XContentType;
import com.paic.dpp.pojo.ElasticInformation;
import com.paic.dpp.pojo.FieldInfo;
import com.paic.dpp.pojo.IndexInformation;
import com.paic.dpp.util.EmptyCollectionException;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * ES客户端
 */
public class ESClient {
    private Logger logger = LoggerFactory.getLogger(ESClient.class);
    private IndexInformation indexInformation;                                    // 操作的索引名
    private ElasticInformation elasticInformation;           // SINGLE集群实例
    public ESClient(IndexInformation indexInformation, ElasticInformation elasticInformation) {
        this.indexInformation = indexInformation;
        this.elasticInformation = elasticInformation;
        try {
            elasticInformation.initConnection();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        logger.info("esclient port:" + elasticInformation.getTcpPort() + ", index:" + indexInformation.getIndexName() + ", type:" + indexInformation.getType()+ ", clusterName:"+elasticInformation.getClusterName());
    }

    /**
     * 判断索引是否存在
     * @return
     */
    public boolean isExistsIndex(){
        return elasticInformation.getClient().admin().indices().prepareExists(indexInformation.getIndexName()).execute().actionGet().isExists();
    }

    /* 创建Index */
    public void createNewIndex() throws Exception {
        //setting
        Settings.Builder setting = makeSettings();
        //mapping
        XContentBuilder mbuilder = makeContent();
        elasticInformation.getClient().admin().indices()
                .prepareCreate(indexInformation.getIndexName())
                .setSettings(setting)
                .addMapping(indexInformation.getType(),mbuilder)
                .execute().actionGet();
        logger.info("esclient create index:"+ indexInformation.getIndexName()+
                ", setting:" + indexInformation.getSettings().toJSONString()+
                ", mapping:" + indexInformation.getMappings());

    }

    private XContentBuilder makeContent() throws IOException {
        XContentBuilder mbuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject(indexInformation.getType());
        for (Map.Entry<String, Object> entry : indexInformation.getMappings().entrySet()) {
            mbuilder.field(entry.getKey(),entry.getValue());
        }

        //property
        mbuilder.startObject("properties");
        for (FieldInfo fieldInfo : indexInformation.getFieldInfoList()) {
            String fieldName = fieldInfo.getFieldName();
            String fieldType = fieldInfo.getFieldType();
            List<FieldInfo> nestedFields = fieldInfo.getNestedFields();
            if(fieldType == null || "".equals(fieldType.trim()))
                fieldType = XContentType.STRING;
            fieldType = fieldType.toLowerCase();
            int participle = fieldInfo.getParticiple();
            //string date nested other
            if(XContentType.STRING.equals(fieldType) && participle == 1){
                mbuilder.startObject(fieldName)
                        .field("type",XContentType.TEXT)
                        .startObject("fields")
                        .startObject(XContentType.KEYWORD)
                        .field("type",XContentType.KEYWORD)
                        .field("ignore_above",256)
                        .endObject()
                        .endObject()
                        .endObject();
            }else if(XContentType.STRING.equals(fieldType) && participle != 1){
                mbuilder.startObject(fieldName)
                        .field("type",participle == 0 ?XContentType.KEYWORD:XContentType.TEXT)
                        .endObject();
            }else if(XContentType.DATE.equals(fieldType)){
                mbuilder.startObject(fieldName)
                        .field("type",XContentType.DATE)
                        .field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                        .endObject();
            }else if(XContentType.NESTED.equals(fieldType)){
                if(indexInformation.isOpenNested())
                    mbuilder.startObject(fieldName)
                        .field("type",XContentType.NESTED)
                        .startObject("properties");
                else
                    mbuilder.startObject(fieldName)
                            .startObject("properties");
                for (FieldInfo nestedField : nestedFields) {
                    if(XContentType.STRING.equals(nestedField.getFieldType()) && nestedField.getParticiple() == 1){
                        mbuilder.startObject(nestedField.getFieldName())
                                .field("type",XContentType.TEXT)
                                .startObject("fields")
                                .startObject(XContentType.KEYWORD)
                                .field("type",XContentType.KEYWORD)
                                .field("ignore_above",256)
                                .endObject()
                                .endObject()
                                .endObject();
                    }else if(XContentType.STRING.equals(nestedField.getFieldType()) && nestedField.getParticiple() != 1) {
                        mbuilder.startObject(nestedField.getFieldName())
                                .field("type", nestedField.getParticiple() == 0 ? XContentType.KEYWORD : XContentType.TEXT)
                                .endObject();
                    }else if(XContentType.DATE.equals(nestedField.getFieldType())){
                        mbuilder.startObject(nestedField.getFieldName())
                                .field("type",XContentType.DATE)
                                .field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                                .endObject();
                    }else{
                        mbuilder.startObject(nestedField.getFieldName())
                                .field("type",nestedField.getFieldType())
                                .endObject();
                    }
                }
                mbuilder.endObject().endObject();
            }else{
                mbuilder.startObject(fieldName)
                        .field("type",fieldType)
                        .endObject();
            }
        }
        mbuilder.endObject().endObject().endObject();
        return mbuilder;
    }

    private Settings.Builder makeSettings() {
        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, Object> entry : indexInformation.getSettings().entrySet()) {
            builder.put(entry.getKey(),entry.getValue().toString());
        }
//        builder.put("cluster.routing.allocation.disk.watermark.flood_stage", "10mb");
//        builder.put("cluster.routing.allocation.disk.watermark.high", "20mb");
//        builder.put("cluster.routing.allocation.disk.watermark.low", "30mb");
        builder.put("number_of_shards",1);
        builder.put("number_of_replicas",1);
        builder.put("index.mapping.total_fields.limit",5000);
        builder.put("refresh_interval",-1);
        return builder;
    }

    /* 修改indexsetting */
    public void putSetting(String key, String value) {

        Settings.Builder builder = Settings.builder();
        builder.put(key,value);
        elasticInformation.getClient().admin().indices().prepareUpdateSettings(indexInformation.getIndexName())
                .setSettings(builder).execute().actionGet();
        logger.info("Es client put setting:" + key+"/"+value);
    }


    /* forceMerge索引，将segment数目降低为1个 */
    public void forceMerge() throws Exception {
        elasticInformation.getClient().admin().indices().prepareForceMerge(indexInformation.getIndexName())
                .setFlush(true)
                .setMaxNumSegments(1)
                .execute()
                .actionGet();
        logger.info("es client force merge is done" );
    }

    /* flush索引 */
    public boolean flush() throws Exception {
        FlushResponse flushResponse = elasticInformation.getClient().admin().indices().prepareFlush(indexInformation.getIndexName())
                .execute().actionGet();
        int failedShards = flushResponse.getFailedShards();
        logger.info("esclient flush failed shards :" + failedShards);
        if(failedShards == 0)
            return true;
        else
            return false;
    }

    /* refresh索引 */
    public boolean refresh() throws Exception {
        RefreshResponse refreshResponse = elasticInformation.getClient().admin().indices().prepareRefresh(indexInformation.getIndexName())
                .execute().actionGet();
        int failedShards = refreshResponse.getFailedShards();
        logger.info("esclient refresh failed shards :" + failedShards);
        if(failedShards == 0)
            return true;
        else
            return false;
    }



    /* 写入数据 */
    public void multiWrite(Iterator<DocData> docData) {
        long start = System.currentTimeMillis();
        Client client = elasticInformation.getClient();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int count = 0;
        while(docData.hasNext()){
            DocData docDatum = docData.next();
            bulkRequest.add(client.prepareIndex(indexInformation.getIndexName(), indexInformation.getType())
                    .setId(docDatum.key == null ? UUID.randomUUID().toString():docDatum.key)
                    .setSource(docDatum.data));
            count ++ ;
            if(count % 10000 == 0){
                BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
                if(bulkItemResponses.hasFailures()){
                    String msg = bulkItemResponses.buildFailureMessage();
                    logger.error("Bulk has failed message: "+msg);
                }
                bulkRequest = client.prepareBulk();
            }
        }
        BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
        if(bulkItemResponses.hasFailures()){
            String msg = bulkItemResponses.buildFailureMessage();
            logger.error("Bulk has failed message: "+msg);
        }
        long cost = System.currentTimeMillis()-start;
        if(cost>10000) {
            logger.info("cost time to much, cost:" + cost + "ms");
        }
        logger.info("Bulk data is done!");
    }

    /* 写入数据 */
    public void multiWriteAsRequest(List<DocData> docData) throws EmptyCollectionException {
        long start = System.currentTimeMillis();
        Client client = elasticInformation.getClient();
        BulkRequest request = new BulkRequest();
        int count = 0;
        for (DocData docDatum : docData) {
            IndexRequest indexRequest = new IndexRequest(indexInformation.getIndexName(),
                    indexInformation.getType()
            )
                    .id(docDatum.key == null ? UUID.randomUUID().toString():docDatum.key)
                    .source(docDatum.data.getInnerMap());
            request.add(indexRequest);
            count ++ ;
            if(count % 10000 == 0){
                BulkResponse bulkItemResponses = client.bulk(request).actionGet();
                if(bulkItemResponses.hasFailures()){
                    String msg = bulkItemResponses.buildFailureMessage();
                    logger.error("Bulk has failed message: "+msg);
                }
                request = new BulkRequest();
            }
        }
        if(count == 0) {
            logger.error("Bulk data is empty!!");
            throw new EmptyCollectionException("empty data exception");
        }
        BulkResponse bulkItemResponses = client.bulk(request).actionGet();
        if(bulkItemResponses.hasFailures()){
            String msg = bulkItemResponses.buildFailureMessage();
            logger.error("Bulk has failed message: "+msg);
        }
        long cost = System.currentTimeMillis()-start;
        if(cost>10000) {
            logger.info("cost time to much, cost:" + cost + "ms");
        }
        logger.info("Bulk data is done!");
    }

    public static class DocData {
        public String key;
        public JSONObject data;
    }
}
