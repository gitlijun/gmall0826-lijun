package com.atguigu.gmall2019.canal;/*
  需求：
  思路：
   技术点：
**/


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {
        //todo 1 连接canalserver
        //获取连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop11", 11111), "example", "", "");
        while (true) {//常住内存的，实时监控
            canalConnector.connect();
            //todo 2 抓取日志数据
            //bilog 会把此库下的表都写入到一个binlog钟，因此需要过滤出想要的表
            canalConnector.subscribe("*.*");
            Message message = canalConnector.get(100);
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {//如果有数据，就遍历取出来解析日志

                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (CanalEntry.EntryType.ROWDATA == entry.getEntryType()) {
                        ByteString storeValue = entry.getStoreValue();//序列化数据
                        CanalEntry.RowChange rowChange=null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);//反序列化
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集合
                        CanalEntry.EventType eventType = rowChange.getEventType();//行操作类型（dml）
                        String tableName = entry.getHeader().getTableName();//表名

                        //todo 3 对日志进行解析成想要的格式
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);

                        //todo 4 分流不同业务主题发送到kafka
                        canalHandler.handle();


                    }
                }
            }



        }


    }


}
