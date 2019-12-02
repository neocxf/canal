package com.alibaba.otter.canal.example.simple;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于canal的数据处理器
 *
 * @author idea
 * @data 2019/10/13
 */
@Slf4j
public class CanalDataHandler extends TypeConvertHandler {


    /**
     * 将binlog的记录解析为一个bean对象
     *
     * @param columnList
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> T convertToBean(List<CanalEntry.Column> columnList, Class<T> clazz) {
        T bean = null;
        try {
            bean = clazz.newInstance();
            Field[] fields = clazz.getDeclaredFields();
            Field.setAccessible(fields, true);
            Map<String, Field> fieldMap = new HashMap<>(fields.length);
            for (Field field : fields) {
                fieldMap.put(field.getName().toLowerCase(), field);
            }
            if (fieldMap.containsKey("serialVersionUID")) {
                fieldMap.remove("serialVersionUID".toLowerCase());
            }
            System.out.println(fieldMap.toString());
            for (CanalEntry.Column column : columnList) {
                String columnName = column.getName();
                String columnValue = column.getValue();
                System.out.println(columnName);

                Field field = fieldMap.get(columnName);

                Object val = PropertiesLoader.castPrimitive(field, columnValue);

                field.set(bean, val);

//                if (fieldMap.containsKey(columnName)) {
//                    //基础类型转换不了
//                    Field field = fieldMap.get(columnName);
//                    Class<?> type = field.getType();
//                    if(BEAN_FIELD_TYPE.containsKey(type)){
//                        switch (BEAN_FIELD_TYPE.get(type)) {
//                            case "Integer":
//                                field.set(bean, parseToInteger(columnValue));
//                                break;
//                            case "Long":
//                                field.set(bean, parseToLong(columnValue));
//                                break;
//                            case "Double":
//                                field.set(bean, parseToDouble(columnValue));
//                                break;
//                            case "String":
//                                field.set(bean, columnValue);
//                                break;
//                            case "java.handle.Date":
//                                field.set(bean, parseToDate(columnValue));
//                                break;
//                            case "java.sql.Date":
//                                field.set(bean, parseToSqlDate(columnValue));
//                                break;
//                            case "java.sql.Timestamp":
//                                field.set(bean, parseToTimestamp(columnValue));
//                                break;
//                            case "java.sql.Time":
//                                field.set(bean, parseToSqlTime(columnValue));
//                                break;
//                        }
//                    }else{
//                        field.set(bean, parseObj(columnValue));
//                    }
//                }

            }
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("[CanalDataHandler]convertToBean，初始化对象出现异常，对象无法被实例化,异常为{}", e);
        }
        return bean;
    }


    public static void main(String[] args) throws IllegalAccessException {
        UserDto courseDetailDTO = new UserDto();
        Class clazz = courseDetailDTO.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Field.setAccessible(fields, true);
        System.out.println(courseDetailDTO);
        for (Field field : fields) {
            if ("java.lang.String".equals(field.getType().getName())) {
                field.set(courseDetailDTO, "name");
            }
        }
        System.out.println(courseDetailDTO);
    }


    /**
     * 其他类型自定义处理
     *
     * @param source
     * @return
     */
    public static Object parseObj(String source){
        return null;
    }


    @Data
    static class UserDto {
        long id;
        String name;
        long age;
    }
}