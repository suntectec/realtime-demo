package com.sands.realtime.common.base;

import lombok.extern.slf4j.Slf4j;

/**
 * Starter 的设计初衷：
 *  启动 Flink 流策略或者流表策略执行环境
 *  使用方法可以是 extend BaseAPP 继承，或者 Starter 调用（无主类名）
 *
 * @author Jagger
 * @since 2025/9/17 9:18
 */
@Slf4j
public class Starter {
    private final IBaseAPP baseAPP;

    public Starter(IBaseAPP baseAPP) {
        this.baseAPP = baseAPP;
    }

    public void start(int port, String[] args) throws Exception {
        if (baseAPP instanceof BaseStreamEnvAPP) {
            BaseStreamEnvAPP baseStreamAPP = (BaseStreamEnvAPP) baseAPP;
            baseStreamAPP.printLog();
        } else if (baseAPP instanceof BaseTableEnvAPP) {
            BaseTableEnvAPP baseTableAPP = (BaseTableEnvAPP) baseAPP;
            baseTableAPP.printLog();
        }

        baseAPP.start(port, args);

    }
}
