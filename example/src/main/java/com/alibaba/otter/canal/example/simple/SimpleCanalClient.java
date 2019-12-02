package com.alibaba.otter.canal.example.simple;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

public class SimpleCanalClient {
    private static String SERVER_ADDRESS = "127.0.0.1";
    private static Integer PORT = 11111;
    private static String DESTINATION = "example";
    private static String USERNAME = "";
    private static String PASSWORD = "";
    public static void main(String[] args)  {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(SERVER_ADDRESS, PORT), DESTINATION, USERNAME, PASSWORD);
        canalConnector.connect();
        canalConnector.subscribe(".*\\..*");
        canalConnector.rollback();
        for (; ; ) {
            Message message = canalConnector.getWithoutAck(100);
            long batchId = message.getId();
            if(batchId!=-1){
                System.out.println(batchId);
                printEntity(message.getEntries());
            }
        }
    }
    public static void printEntity(List<CanalEntry.Entry> entries){
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType()!=CanalEntry.EntryType.ROWDATA){
                continue;
            }
            try {
                CanalEntry.RowChange rowChange=CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    System.out.println(rowChange.getEventType());
                    switch (rowChange.getEventType()){
                    //如果希望监听多种事件，可以手动增加case
                        case INSERT:
                            String tableName = entry.getHeader().getTableName();
                            //测试选用t_type这张表进行映射处理
                            if ("users".equals(tableName)) {
                                CanalDataHandler.UserDto typeDTO = CanalDataHandler.convertToBean(rowData.getAfterColumnsList(), CanalDataHandler.UserDto.class);
                                System.out.println(typeDTO);
                            }
                            System.out.println("this is INSERT");
                            break;
                        default:
                            break;
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 打印内容
     *
     * @param columns
     */
    private static void printColums(List<CanalEntry.Column> columns){
        String line=columns.stream().map(column -> column.getName()+"="+column.getValue())
                .collect(Collectors.joining(","));
        System.out.println(line);
    }
}