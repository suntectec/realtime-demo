package com.sands.realtime.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * Merge two ParameterTools, with Priority of setting Global Job Parameters:
 *  args input -> application.properties
 *
 * @author Jagger
 * @since 2025/8/18 22:53
 */
public abstract class ParametersUtil {

    /**
     * Program arguments: --name zhangshan --age 20 --warehouse xxx
     *
     * @param streamEnv 流执行环境
     * @param args Higher Priority args Input, Secondary Priority properties File
     */
    public static ParameterTool setGlobalJobParameters(StreamExecutionEnvironment streamEnv, String[] args) throws IOException {

        // 1. 全局变量获取 application.properties
        ParameterTool parameter1 = PropertiesUtil.getPropertiesParameters();

        // 2. 全局变量获取 args
        ParameterTool parameter2 = ParameterTool.fromArgs(args);

        // 合并（parameters2 的参数会覆盖 parameters1 的同名参数）
        ParameterTool mergedParam = parameter1.mergeWith(parameter2);

        streamEnv.getConfig().setGlobalJobParameters(mergedParam);

        return mergedParam;
    }

}