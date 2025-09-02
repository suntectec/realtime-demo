package com.sands.realtime.ods.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 自定义日期格式转换器：
 *  debezium会将日期转为5位数字，日期时间位13位的数字，因此我们需要根据Sqlserver的日期类型转换成标准的时期或者时间格式
 *
 * @author Jagger
 * @since 2025/8/22 15:32
 */
@Slf4j
public class DateTimeDebeziumConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter timeFormatter;
    private DateTimeFormatter datetimeFormatter;
    private SchemaBuilder schemaBuilder;
    private String databaseType;
    private String schemaNamePrefix;

    // 配置sqlserver初始化
    @Override
    public void configure(Properties properties) {
        // 必填参数：database.type，只支持sqlserver
        this.databaseType = properties.getProperty("database.type");
        // 如果未设置，或者设置的不是mysql、sqlserver，则抛出异常。
        if (this.databaseType == null || !this.databaseType.equals("sqlserver")) {
            throw new IllegalArgumentException("database.type 必须设置为'sqlserver'");
        }
        // 选填参数：format.date、format.time、format.datetime。获取时间格式化的格式
        String dateFormat = properties.getProperty("format.date", DATE_FORMAT);
        String timeFormat = properties.getProperty("format.time", TIME_FORMAT);
        String datetimeFormat = properties.getProperty("format.datetime", DATETIME_FORMAT);
        // 获取自身类的包名+数据库类型为默认schema.name
        String className = this.getClass().getName();
        // 查看是否设置schema.name.prefix
        this.schemaNamePrefix = properties.getProperty("schema.name.prefix", className + "." + this.databaseType);
        // 初始化时间格式化器
        dateFormatter = DateTimeFormatter.ofPattern(dateFormat);
        timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
        datetimeFormatter = DateTimeFormatter.ofPattern(datetimeFormat);
    }

    // sqlserver的转换器
    public void registerSqlserverConverter(String columnType, ConverterRegistration<SchemaBuilder> converterRegistration) {
        String schemaName = this.schemaNamePrefix + "." + columnType.toLowerCase();
        schemaBuilder = SchemaBuilder.string().name(schemaName);
        switch (columnType) {
            case "DATE":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.sql.Date) {
                        return dateFormatter.format(((java.sql.Date) value).toLocalDate());
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            case "TIME":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.sql.Time) {
                        return timeFormatter.format(((java.sql.Time) value).toLocalTime());
                    } else if (value instanceof java.sql.Timestamp) {
                        return timeFormatter.format(((java.sql.Timestamp) value).toLocalDateTime().toLocalTime());
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            case "DATETIME":
            case "DATETIME2":
            case "SMALLDATETIME":
            case "DATETIMEOFFSET":
                converterRegistration.register(schemaBuilder, value -> {
                    if (value == null) {
                        return null;
                    } else if (value instanceof java.sql.Timestamp) {
                        return datetimeFormatter.format(((java.sql.Timestamp) value).toLocalDateTime());
                    } else if (value instanceof microsoft.sql.DateTimeOffset) {
                        microsoft.sql.DateTimeOffset dateTimeOffset = (microsoft.sql.DateTimeOffset) value;
                        return datetimeFormatter.format(
                                dateTimeOffset.getOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
                    } else {
                        return this.failConvert(value, schemaName);
                    }
                });
                break;
            default:
                schemaBuilder = null;
                break;
        }
    }


    @Override
    public void converterFor(RelationalColumn relationalColumn, ConverterRegistration<SchemaBuilder> converterRegistration) {
        // 获取字段类型
        String columnType = relationalColumn.typeName().toUpperCase();
        // 根据数据库类型调用不同的转换器
        if (this.databaseType.equals("sqlserver")) {
            this.registerSqlserverConverter(columnType, converterRegistration);
        } else {
            log.warn("不支持的数据库类型: {}", this.databaseType);
            schemaBuilder = null;
        }
    }

    private String getClassName(Object value) {
        if (value == null) {
            return null;
        }
        return value.getClass().getName();
    }

    // 类型转换失败时的日志打印
    private String failConvert(Object value, String type) {
        String valueClass = this.getClassName(value);
        String valueString = valueClass == null ? null : value.toString();
        return valueString;
    }

}
