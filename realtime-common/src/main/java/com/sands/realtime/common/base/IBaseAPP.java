package com.sands.realtime.common.base;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * BaseApp的设计初衷：
 *  因为flink编程的都是 source - Transformation - sink 编程模式，所有使用抽象类进行封装
 *
 * @author Jagger
 * @since 2025/9/16 23:09
 */
public interface IBaseAPP {

    void start (int port, String[] args) throws Exception;

}
