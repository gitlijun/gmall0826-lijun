package com.atguigu.gmall2019.canal;/*
  需求：
  思路：
   技术点：
**/


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall0826.common.constant.GmallConstant;
import com.atguigu.gmall2019.canal.util.MykafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalHandler {

        CanalEntry.EventType eventType;

        String tableName;

        List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
            this.eventType = eventType;
            this.tableName = tableName;
            this.rowDataList = rowDataList;
        }

        public void handle(){
            if(this.rowDataList!=null && this.rowDataList.size()>0){
                if(tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT){
                    rowDateList2Kafka( GmallConstant.KAFKA_TOPIC_ORDER);
                }
            }

        }
        private void rowDateList2Kafka(String topic){
            for (CanalEntry.RowData rowData : rowDataList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : columnsList) {
                    System.out.println(column.getName()+":"+column.getValue());//发送前打印检查一下数据
                    jsonObject.put(column.getName(),column.getValue());

                }
                MykafkaSender.send(topic,jsonObject.toJSONString());
            }
        }


}
