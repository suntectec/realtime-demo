package com.sands.jc;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONReader;
import com.sands.jp.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jagger
 * @since 2025/9/9 9:44
$FLINK_HOME/bin/flink run \
-c com.sands.jc.StudentTest \
$FLINK_HOME/usrlib/realtime-json-children/target/realtime-json-children-1.0-SNAPSHOT.jar
 */
public class StudentTest {
    public static void main(String[] args) throws Exception {

        // Student student = Student.builder()
        //         .id(1)
        //         .name("tom")
        //         .address("beijing")
        //         .phoneNumber("010-6668899")
        //         .build();

        String jsonString = "{" +
                "\"id\":1," +
                "\"name\":\"tom\"," +
                "\"phone_number\":\"010-6668899\"," +
                "\"age_years\":18" +
                "}";

        // 把对象转化为json串
        System.out.println(jsonString);
        Student student1 = JSON.parseObject(jsonString, Student.class);
        System.out.println(student1);
        String jsonString1 = JSON.toJSONString(student1);
        System.out.println(jsonString1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.fromData(jsonString);
        source.print(">source>");

        SingleOutputStreamOperator<Student> objDS = source
                .map(line -> JSON.parseObject(line, Student.class));
        objDS.print(">objDS>");

        SingleOutputStreamOperator<String> strDS = objDS.map(JSON::toJSONString);
        strDS.print(">strDS>");

        env.execute();
        env.setParallelism(1);
        env.setMaxParallelism(1);

    }
}
