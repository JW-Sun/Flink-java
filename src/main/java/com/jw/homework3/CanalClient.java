package com.jw.homework3;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.159.102", 11111), "example", "", "");
        while (true) {
            canalConnector.connect();
            // 监控订阅对应的表
            canalConnector.subscribe("test_flink_canal.source");

            // 每次抓取50条消息
            Message message = canalConnector.get(50);
            int size = message.getEntries().size();
            if (size == 0) {
                System.out.println("没有数据，等待");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("开始处理Canal数据");
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 判断事件类型只处理行变化。
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = null;
                        try {
                            // 反序列化成RowChange实例对象
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        // 获取操作类型字段
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 获取binlog文件名称
                        String logfileName = entry.getHeader().getLogfileName();
                        // 当前操作在binlog中的位置
                        long logfileOffset = entry.getHeader().getLogfileOffset();
                        // 获得当前操作的数据库
                        String schemaName = entry.getHeader().getSchemaName();
                        // 获得当前操作的数据库表
                        String tableName = entry.getHeader().getTableName();
                        // 执行时间
                        long executeTime = entry.getHeader().getExecuteTime();

                        // 解析行的数据
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            // 添加或更新后 获得新增后的所有列数据
                            if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                                handle(rowData.getAfterColumnsList(), logfileName, logfileOffset, schemaName, eventType, executeTime);
                            } else {
                                // 删除操作后
                                handle(rowData.getBeforeColumnsList(), logfileName, logfileOffset, schemaName, eventType, executeTime);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void handle(List<CanalEntry.Column> columnList,
                               String logfileName,
                               long logfileOffset,
                               String schemaName,
                               CanalEntry.EventType eventType,
                               long executeTime) {

        JSONObject jsonObject = new JSONObject();
        // 基本信息封装
        StringBuilder bd = new StringBuilder();
        bd.append(logfileName).append("-").append(logfileOffset).append("-").append(schemaName).append("-")
                .append(executeTime).append("-").append(eventType);
        jsonObject.put("base_info", bd.toString());

        // 对于每一行中每一列的数据进行分析
        for (CanalEntry.Column column : columnList) {
            System.out.println(column.getName() + " " + column.getValue());
            // 将每一列的情况进行封装
            jsonObject.put(column.getName(), column.getValue());
        }
        // kafka生产者发送消息。
        MyKafkaProducer.send("test_flink_canal", jsonObject.toJSONString());

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
