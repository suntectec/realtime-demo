package com.sands.realtime.ods.app;

import com.sands.realtime.common.utils.ResourcesFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 优化后读取 sql-scripts 文件
 *
 * @author Jagger
 * @since 2025/8/13 10:10
 */
@Slf4j
public class OdsSqlserverToPaimonS3APP {

    public static void main(String[] args) throws IOException {
        String sqlFilePath = "scripts/sqlserver2paimon.s3.sql";

        try {
            // 读取并解析SQL文件
            List<String> sqlStatements = parseSQLFile(sqlFilePath);

            // 执行SQL语句
            executeSQLStatements(sqlStatements);

            log.info("SQL文件执行完成！");
        } catch (IOException e) {
            // System.err.println("读取SQL文件失败: " + e.getMessage());
            log.error("读取SQL文件失败: " + e.getMessage());
        } catch (SQLException e) {
            // System.err.println("数据库操作失败: " + e.getMessage());
            log.error("数据库操作失败: " + e.getMessage());
        }
    }

    /**
     * 解析SQL文件，过滤注释并分割SQL语句
     */
    private static List<String> parseSQLFile(String filePath) throws IOException {
        List<String> statements = new ArrayList<>();
        StringBuilder currentStatement = new StringBuilder();

        // 读取文件内容
        ResourcesFileReader reader = new ResourcesFileReader();
        String content = reader.read(filePath);

        String[] lines = content.split("\r\n|\r|\n");

        for (String line : lines) {
            // 移除行首尾空白字符
            line = line.trim();

            // 跳过空行和注释行
            if (line.isEmpty() || line.startsWith("--")) {
                continue;
            }

            // 移除行内注释（如果有）
            int commentIndex = line.indexOf("--");
            if (commentIndex != -1) {
                line = line.substring(0, commentIndex).trim();
            }

            // 将行添加到当前语句
            currentStatement.append(line).append(" ");

            // 如果行以分号结尾，则完成一个语句
            if (line.endsWith(";")) {
                String sql = currentStatement.toString().trim();
                // 移除末尾的分号（某些JDBC驱动不需要分号）
                if (sql.endsWith(";")) {
                    sql = sql.substring(0, sql.length() - 1);
                }
                statements.add(sql);
                currentStatement.setLength(0); // 重置StringBuilder
            }
        }

        // 处理文件末尾没有分号的情况
        if (currentStatement.length() > 0) {
            String sql = currentStatement.toString().trim();
            if (sql.endsWith(";")) {
                sql = sql.substring(0, sql.length() - 1);
            }
            statements.add(sql);
        }

        return statements;
    }

    /**
     * 执行SQL语句列表
     */
    private static void executeSQLStatements(List<String> sqlStatements) throws SQLException{

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(10000);
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        for (String sql : sqlStatements) {
            if (!sql.isEmpty()) {
                log.info("执行SQL: " + sql);
                TableResult tableResult = tableEnv.executeSql(sql);
                if (tableResult.getJobClient().isPresent()) log.info("提交作业状态：" + tableResult.getJobClient().get().getJobStatus());
            }
        }

    }

}
