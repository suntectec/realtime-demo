package com.sands.realtime.ods.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author Jagger
 * @since 2025/9/16 17:57
 */
public class Test {
    public static void main(String[] args) {
        try {
            // 读取第一个 SQL 文件
            String sql1 = Files.readString(Paths.get("sink-table.sql"));

            // 读取第二个 SQL 文件
            String sql2 = Files.readString(Paths.get("source-table.sql"));

            System.out.println("第一个 SQL 文件内容：");
            System.out.println(sql1);

            System.out.println("\n第二个 SQL 文件内容：");
            System.out.println(sql2);

        } catch (IOException e) {
            System.err.println("读取文件时出错: " + e.getMessage());
        }
    }
}
