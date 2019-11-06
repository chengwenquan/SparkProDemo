package com.cwq.spark.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class JDBCTool {

    public static void ReflectDemo(Object object){
        String simpleName = object.getClass().getSimpleName();
        System.out.println("类名："+simpleName);

        Field[] declaredFields = object.getClass().getDeclaredFields();
        for (Field field:declaredFields){
            System.out.println("-------------------");
            String modifier = Modifier.toString(field.getModifiers());
            System.out.println("修饰符："+modifier);
            System.out.println("类型:"+field.getType().getSimpleName());
            System.out.println("属性名："+field.getName());
            System.out.println("-------------------");
        }

        Method[] methods = object.getClass().getDeclaredMethods();
        for (Method method:methods){
            //方法修饰
            String methodModifier = Modifier.toString(method.getModifiers());
            //方法类型
            String methodType = method.getReturnType().getSimpleName();
            //方法名
            String methodName = method.getName();
            System.out.println(methodModifier+" "+methodType+" "+methodName);
        }
    }

    public static <T> List<T> selectAll(String sql, T obj){
        Connection client = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        ArrayList<T> list = new ArrayList<>();
        try {
            //获取对象的所有属性
            Field[] fields = obj.getClass().getDeclaredFields();
            //获取数据库连接
             client = DataSourceUtil.getConnection();
             ps = client.prepareStatement(sql);
             resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Object record = obj.getClass().newInstance();
                for (Field field : fields) {//循环赋值
                    field.setAccessible(true);//private是否可见，必须要有
                    String type = field.getType().getSimpleName();
                    switch (type) {
                        case "String":
                            field.set(record, resultSet.getString(field.getName()));
                            break;
                        case "int":
                            field.set(record, resultSet.getInt(field.getName()));
                            break;
                    }
                }
                list.add((T) record);
            }
        }catch (Exception e){
            System.out.println("------异常-----");
        }finally {
            try {
                if (ps!=null) ps.close();
                if(resultSet!=null) resultSet.close();
                if(client!=null) client.close();
            }catch (Exception e){
            }
        }
        return list;
    }



    public static void main(String[] args) throws Exception{
        List<User> list = selectAll("select id,name from user", new User());
        list.forEach(System.out::println);
    }
}
