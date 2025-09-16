package com.sands.realtime.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 资源文件读取工具类
 * 用于读取 classpath 下 resources 目录中的文件
 *
 * @author Jagger
 * @since 2025/9/16 20:42
 */
@Slf4j
public class ResourcesFileReader {

    private ResourcesFileReader() {
        // 工具类，防止实例化
    }

    /**
     * 读取资源文件内容（默认使用 UTF-8 编码）
     *
     * @param filePath 文件路径，相对于 resources 目录，如 "sql/query.sql"
     * @return 文件内容字符串
     * @throws IOException 如果文件不存在或读取失败
     */
    public static String readResourcesFile(String filePath) throws IOException {
        try (var inputStream = ResourcesFileReader.class.getClassLoader().getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new IOException("文件未找到: " + filePath);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

}
