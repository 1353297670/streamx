package com.streamxhub.streamx.flink.core.java.operator;


import com.streamxhub.streamx.common.conf.ConfigConst;
import com.streamxhub.streamx.common.util.JdbcUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.vertx.core.impl.Arguments.require;

/**
 * @author hx
 * @date 2021-11-15 13:50
 * @desc 自定义 jdbc-sink Operator,至少一次语义，将一个批次的连续插入sql数据合并输出
 */
public class JdbcSinkMergeSqlOperator<T extends Serializable> extends AbstractStreamOperator<Object> implements ProcessingTimeCallback, OneInputStreamOperator<T, Object>{
    private Map<String,List<String>> map;
    private ListState<Map> listState;
    private int batchSize;
    private long interval;
    private Connection connection;
    private Statement statement;
    private ProcessingTimeService processingTimeService;
    private Properties jdbc;
    public Logger logger ;

    public JdbcSinkMergeSqlOperator(Properties jdbc) {

        Object size = jdbc.remove(ConfigConst.KEY_JDBC_INSERT_BATCH());
        Object time = jdbc.remove(ConfigConst.KEY_JDBC_INSERT_INTERVAL());

        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = size == null ? ConfigConst.DEFAULT_JDBC_INSERT_BATCH() : Integer.parseInt(size.toString());
        this.interval = time == null ? ConfigConst.DEFAULT_JDBC_INSERT_INTERVAL() : Long.parseLong(time.toString());
        this.jdbc = jdbc;
    }

    public JdbcSinkMergeSqlOperator(int batchSize, long interval, Properties jdbc) {

        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = batchSize;
        this.interval = interval;
        this.jdbc = jdbc;
    }

    @Override
    public void open() throws Exception {
        logger.info("JdbcSinkMergeSqlOperator:初始化open方法执行");
        super.open();
        require(jdbc != null, "[StreamX] JdbcSink jdbc can not be null");
        jdbc.remove(ConfigConst.KEY_JDBC_INSERT_BATCH());
        jdbc.remove(ConfigConst.KEY_JDBC_INSERT_INTERVAL());
        connection = JdbcUtils.getConnection(jdbc);
        connection.setAutoCommit(false);
        if (interval > 0 && batchSize > 1) {
            statement = connection.createStatement();
            processingTimeService = getProcessingTimeService();
            long now = processingTimeService.getCurrentProcessingTime();
            processingTimeService.registerTimer(now + interval, this);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        logger = LoggerFactory.getLogger(JdbcSinkMergeSqlOperator.class);
        logger.info("JdbcSinkMergeSqlOperator:初始化");
        super.initializeState(context);
        this.map = new ConcurrentHashMap<>();
        listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("jdbc-interval-sink",Map.class));
        if (context.isRestored()) {
            logger.info("JdbcSinkMergeSqlOperator:状态恢复开始");
            listState.get().forEach(x -> map.putAll(x));
            logger.info("JdbcSinkMergeSqlOperator:状态恢复完毕");
        }
    }

    @Override
    public void processElement(StreamRecord<T> element) {
        String sql = element.getValue().toString();
        logger.debug("execute sql: " + sql);

        if (batchSize == 1) {
            try {
                statement = connection.prepareStatement(sql);
                ((PreparedStatement) statement).executeUpdate();
                connection.commit();
            } catch (SQLException e) {
                logger.error( "JdbcSinkMergeSqlOperator invoke error:" + sql);
                e.printStackTrace();
            }
        } else {
            //解析sql语句中的库、表名 或者表名 作为存储的key
            String key = matchSql(sql);
            List list = map.get(key) == null ? new ArrayList() : map.get(key);
            try {

                if (sql.startsWith("insert") || sql.startsWith("INSERT")) {
                    list.add(sql);
                    map.put(key, list);
                    if (list.size() >= batchSize) {
                        execBatch(list);
                        map.remove(key);
                    }
                } else {
                    execBatch(list);
                    map.remove(key);
                    statement.executeUpdate(sql);
                    connection.commit();
                }

            } catch (Exception e) {
                logger.error("JdbcSinkMergeSqlOperator execute error:" + list);
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        listState.clear();
        if (map.size() > 0) {
            listState.add(map);
        }
    }

    @Override
    public void onProcessingTime(long timestamp) {
        logger.debug("定时器触发：" + processingTimeService.getCurrentProcessingTime());

        Set<Map.Entry<String, List<String>>> entries = map.entrySet();

        Iterator<Map.Entry<String, List<String>>> iterator = entries.iterator();

        while (iterator.hasNext()) {
            Map.Entry<String,List<String>> listEntry = iterator.next();

            String key = listEntry.getKey();
            List value = listEntry.getValue();
            if (value.size() > 0) {
                try {
                    execBatch(value);
                    map.remove(key);
                } catch (Exception e) {
                    logger.error("JdbcSinkMergeSqlOperator execute error:" + value);
                    throw new RuntimeException(e.getMessage());
                }
            }
        }

        long now = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(now + interval, this);
    }

    @Override
    public void close() throws Exception {
        super.close();

        map.values().forEach(list -> {
            if (list.size() > 0) {
                try {
                    execBatch(list);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }

        });
        statement.close();
        connection.close();

    }
    public void execBatch(List list) throws Exception {
        if (list.size() > 0) {
            Long start = System.currentTimeMillis();
            StringBuilder mergeSql = mergeSql(list);
            logger.debug("合并后的sql：" + mergeSql);
            statement.executeUpdate(mergeSql.toString());
            connection.commit();
            Long end = System.currentTimeMillis();
            logger.info("执行批次大小：" + list.size() + ", 执行时长：" + (end - start));
        }
    }

    /**
     * @param list
     * @return
     * @desc 将一个批次的insert语句合并为一个大的insert事件
     */
    public StringBuilder mergeSql(List list) {
        StringBuilder mergeSql = new StringBuilder();
        if (list.size() > 0) {
            for (int i = 0; i < list.size(); i++) {
                if (i == 0) {
                    mergeSql.append(list.get(i));
                } else {
                    mergeSql.append("," + list.get(i).toString().split("values|VALUES")[1]);
                }
            }
        }
        return mergeSql;
    }

    /**
     *
     * @param sql
     * @return
     */
    private String matchSql(String sql){
        Matcher matcher = null;
        //SELECT 列名称 FROM 表名称
        //SELECT * FROM 表名称
        if (sql.startsWith("select")) {
            matcher = Pattern.compile("select\\s.+from\\s(.+)where\\s(.*)").matcher(sql);
            if (matcher.find()) return matcher.group(1);
        }
        //INSERT INTO 表名称 VALUES (值1, 值2,....)
        //INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
        if (sql.startsWith("insert")) {
            matcher = Pattern.compile("insert\\sinto\\s(.+)\\s\\(.*\\)\\svalues\\s.*").matcher(sql);
            if (matcher.find()) return matcher.group(1);
        }
        //UPDATE 表名称 SET 列名称 = 新值 WHERE 列名称 = 某值
        if (sql.startsWith("update")) {
            matcher = Pattern.compile("update\\s(.+)set\\s.*").matcher(sql);
            if (matcher.find()) return matcher.group(1);
        }
        //DELETE FROM 表名称 WHERE 列名称 = 值
        if (sql.startsWith("delete")) {
            matcher = Pattern.compile("delete\\sfrom\\s(.+)where\\s(.*)").matcher(sql);
            if (matcher.find()) return matcher.group(1);
        }
        return null;
    }
}
