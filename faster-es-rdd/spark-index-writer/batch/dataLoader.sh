#!/bin/sh

#1. create index
curl -XPUT 'http://master:9200/user_dimension' -H 'Content-Type:application/json' -d '{ "settings":{"number_of_shards":1,"number_of_replicas":1}, "mappings":{ "doc":{ "date_detection":false, "properties":{ "birth" : { "type" : "date", "format" : "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis" }, "gender" : { "type" : "keyword" }, "name" : { "type" : "text", "fields" : { "keyword" : { "type" : "keyword", "ignore_above" : 256 } } }, "provice" : { "type" : "text", "fields" : { "keyword" : { "type" : "keyword", "ignore_above" : 256 } } }, "province" : { "type" : "text", "fields" : { "keyword" : { "type" : "keyword", "ignore_above" : 256 } } }, "uid" : { "type" : "text", "fields" : { "keyword" : { "type" : "keyword", "ignore_above" : 256 } } } } } } }'
#2. close index
curl -XPOST 'http://master:9200/user_dimension/_close'
#3. data copy
### index & translog文件替换
#4. open index
curl -XPOST 'http://master:9200/user_dimension/_open'
#5. index red status()
# 查看分配失败原因curl -XGET -H 'Content-Type:application/json' 'http://master:9200/_cluster/allocation/explain' -d '{"index":"user_dimension","shard":0,"primary":true}'
# 开启自动分配 curl -XPUT  -H 'Content-Type:application/json' 'master:9200/_cluster/settings' -d '{ "transient" : { "cluster.routing.allocation.enable" : "all" }}'