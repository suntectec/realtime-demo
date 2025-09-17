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
public class ResourcesFileReader {

    /**
     * 类加载器
     */
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    public ResourcesFileReader() {
    }

    public ResourcesFileReader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * 使用指定类加载器读取资源文件（默认使用 UTF-8 编码）
     *
     * @param filePath 文件路径，相对于 resources 目录，如 "sql/query.sql"
     * @return 文件内容字符串
     * @throws IOException 如果文件不存在或读取失败
     */
    public String read(String filePath) throws IOException {
        try (var inputStream = classLoader.getResourceAsStream(filePath)) {
            if (inputStream == null) {
                throw new IOException("文件未找到: " + filePath);
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

}
