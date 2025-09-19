package com.sands.realtime.common.base;

/**
 * IBaseApp 的设计初衷：
 *  Flink 执行环境实施策略接口
 *
 * @author Jagger
 * @since 2025/9/17 9:17
 */
public interface IBaseAPP {
    void start(int port, String[] args) throws Exception;
}
