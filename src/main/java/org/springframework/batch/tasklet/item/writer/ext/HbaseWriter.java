package org.springframework.batch.tasklet.item.writer.ext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 *
 * @param <T>
 */
@Slf4j
public abstract class HbaseWriter<T> implements ItemWriter<T>, InitializingBean,
        StepExecutionListener {

    /** Hbase connection */
    protected Connection connection;
    /** Hbase configuration */
    @Setter
    protected Configuration configuration;

    /** Full table name */
    @Setter
    private String tableName;
    /** Hbase table definition */
    private Table table;

    /*
     * (non-Javadoc)
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(tableName);
        this.connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf(tableName));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.batch.core.StepExecutionListener#beforeStep(org.
     * springframework.batch.core.StepExecution)
     */
    @Override
    public void beforeStep(@NonNull final StepExecution stepExecution) {
        // Nothing to do here
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
     */
    @Override
    public void write(@NonNull final List<? extends T> items) throws Exception {
        final List<Put> puts = items.parallelStream().map(this::setPutParameter)
                .collect(Collectors.toList());
        table.put(puts);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.batch.core.StepExecutionListener#afterStep(org.
     * springframework.batch.core.StepExecution)
     */
    @Override
    public ExitStatus afterStep(@NonNull final StepExecution stepExecution) {
        if (table != null) {
            try {
                table.close();
            } catch (final IOException e) {
                log.warn("Unable to close table {} with exception {}",
                        tableName, e.getMessage());
                stepExecution.addFailureException(e);
                return ExitStatus.FAILED;
            }
        }
        return ExitStatus.COMPLETED;
    }

    /**
     * Abstract method converting T object into Hbase put
     *
     * @param bean
     *            bean to insert in database
     * @return put object used to run the insert in Hbase
     */
    protected abstract Put setPutParameter(T bean);
}
