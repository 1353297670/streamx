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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.vertx.core.impl.Arguments.require;

/**
 * @author hx
 * @desc 自定义 jdbc-sink Operator，至少一次语义
 */
public class JdbcSinkOperator<T extends Serializable> extends AbstractStreamOperator<Object> implements ProcessingTimeCallback, OneInputStreamOperator<T, Object>{
    private List<String> list;
    private ListState<String> listState;
    private int batchSize;
    private long interval;
    private Connection connection;
    private Statement statement;
    private ProcessingTimeService processingTimeService;
    private Properties jdbc;
    public Logger logger ;

    public JdbcSinkOperator(Properties jdbc) {

        Object size = jdbc.remove(ConfigConst.KEY_JDBC_INSERT_BATCH());
        Object time = jdbc.remove(ConfigConst.KEY_JDBC_INSERT_INTERVAL());

        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = size == null ? ConfigConst.DEFAULT_JDBC_INSERT_BATCH() : Integer.parseInt(size.toString());
        this.interval = time == null ? ConfigConst.DEFAULT_JDBC_INSERT_INTERVAL() : Long.parseLong(time.toString());
        this.jdbc = jdbc;
    }

    public JdbcSinkOperator(int batchSize, long interval, Properties jdbc) {

        this.chainingStrategy = ChainingStrategy.ALWAYS;
        this.batchSize = batchSize;
        this.interval = interval;
        this.jdbc = jdbc;
    }

    @Override
    public void open() throws Exception {
        logger.info("JdbcSinkOperator:初始化open方法执行");
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
        logger = LoggerFactory.getLogger(JdbcSinkOperator.class);
        logger.info("JdbcSinkOperator:初始化");
        super.initializeState(context);
        this.list = new ArrayList<>();
        listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("jdbc-interval-sink", String.class));
        if (context.isRestored()) {
            logger.info("JdbcSinkOperator:状态恢复开始");
            listState.get().forEach(x -> list.add(x));
            logger.info("JdbcSinkOperator:状态恢复完毕");
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
                logger.error( "JdbcSinkOperator invoke error:" + sql);
                e.printStackTrace();
            }
        } else {
            try {
                list.add(element.getValue().toString());
                statement.addBatch(sql);

                if (list.size() >= batchSize) {
                    try {
                        execBatch(list);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        listState.clear();
        if (list.size() > 0) {
            listState.addAll(list);
        }
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        logger.debug("定时器触发：" + processingTimeService.getCurrentProcessingTime());
        if (list.size() > 0) {
            execBatch(list);
        }
        long now = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(now + interval, this);
    }

    @Override
    public void close() throws Exception {
        super.close();
        execBatch(list);
        statement.close();
        connection.close();

    }

    public void execBatch(List datas) throws Exception{
        if (list.size() > 0) {
            Long start = System.currentTimeMillis();
            int length = statement.executeBatch().length;
            statement.clearBatch();
            connection.commit();
            Long end = System.currentTimeMillis();
            logger.info("执行批次大小：" + length + ", 执行时长：" + (end - start));
            list.clear();
        }
    }
}
