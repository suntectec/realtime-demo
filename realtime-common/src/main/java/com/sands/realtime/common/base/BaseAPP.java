package com.sands.realtime.common.base;

import com.sands.realtime.common.utils.FlinkSourceUtil;
import com.sands.realtime.common.utils.ParametersUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;

/**
 * BaseApp的设计初衷：
 *  因为flink编程的都是 source - Transformation - sink 编程模式，所有使用抽象类进行封装
 *
 * @author Jagger
 * @since 2025/8/18 16:51
 */
@Slf4j
public abstract class BaseAPP {

    /**
     * Transformation(s) 数据转换，核心业务处理逻辑
     *
     * @param env 流执行环境
     * @param dss 数据源流
     * @param parameter 任务全局参数
     * @throws Exception 异常
     */
    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> dss, ParameterTool parameter) throws Exception;

    public void start(int port, String ckAndGroupId, String topic,String[] args, OffsetsInitializer offsetsInitializer) throws Exception {

        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 flink
        System.setProperty("HADOOP_USER_NAME", "flink");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2.1 本地运行环境添加 8081 WebUI
        if (env instanceof LocalStreamEnvironment) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        }

        // 1.3 将ParameterTool的参数设置成全局的参数
        // ParameterTool parameter = ParameterTool.fromArgs(args);
        // env.getConfig().setGlobalJobParameters(parameter);
        // 1.3.1 升级 parameter 采用优先级：args, properties 的方式获取
        ParameterTool parameter = ParametersUtil.setGlobalJobParameters(env, args);
        // 1.4 检查点相关配置
        // 1.4.1 开启 checkpoint
        env.enableCheckpointing(1000);
        // 1.4.2 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.3 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 1.4.4 checkpoint 的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 1.4.5 checkpoint 失败尝试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // 1.4.6 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.7 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointRetention(RETAIN_ON_CANCELLATION);
        // 1.4.8 允许非对齐 checkpoint
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // 1.4.9 checkpoint 存储
        if (env instanceof LocalStreamEnvironment) {  // 在本地运行的逻辑
            // 设置状态后端
            Configuration config = new Configuration();
            // 使用 HashMapStateBackend 作为状态后端
            config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            // Use File as checkpoint storage
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///D:\\tmp\\flink-checkpoints");
            // 增量快照
            config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

            FileSystem.initialize(config, null);

            env.configure(config);

            log.info("flink提交作业模式：--本地");
        } else { // 在集群运行的逻辑
            // 设置状态后端
            Configuration config = new Configuration();
            // 使用 EmbeddedRocksDBStateBackend 作为状态后端
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            // Use S3 as checkpoint storage
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://lakehouse/flink/flink-checkpoints");
            config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "s3://lakehouse/flink/flink-savepoints");
            config.setString("s3.access.key", "minioadmin");
            config.setString("s3.secret.key", "minioadmin");
            config.setString("s3.endpoint", "http://192.168.138.15:9000");
            config.setString("s3.path.style.access", "true");
            // 增量快照
            config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

            FileSystem.initialize(config, null);

            env.configure(config);

            log.info("flink提交作业模式：--集群");
        }

        // 1.5 从 Kafka 目标主题读取数据，封装为流
        String kafkaServer = parameter.get("kafka.broker");
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(kafkaServer, ckAndGroupId, topic, offsetsInitializer);

        // 2. 读取数据
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 3. 核心业务处理逻辑  4. 数据输出
        handle(env, stream, parameter);

        // 5. execute触发执行
        env.execute();

    }

    /**
     * 本地 8081 WebUI 和 socket文本流 测试
     * @param args 命令行参数
     * @param socketTextStreamPort socket文本流端口号
     * @throws Exception 异常
     */
    public void localStart(String[] args, int socketTextStreamPort) throws Exception {
        // 1. 准备环境
        // 本地测试WebUI
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2. 读取数据
        // Source 接收一个socket文本流
        DataStreamSource<String> dss = env.socketTextStream("localhost", socketTextStreamPort);

        ParameterTool parameter = ParametersUtil.setGlobalJobParameters(env, args);

        // 3. 核心业务处理逻辑  4. 数据输出
        handle(env, dss, parameter);

        // 5. execute触发执行
        env.execute();

    }
}
