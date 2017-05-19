package org.springframework.batch.tasklet.item.reader.ext;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import lombok.NonNull;
import lombok.Setter;

/**
 * Spring batch reader that stream result of hbase scans
 *
 * @param <T>
 *            type of read data
 */
public class HbaseReader<T> implements ItemReader<T>, InitializingBean {

    /** List of scans to stream */
    @Setter
    private List<Scan> scans;
    /** Table name to query */
    @Setter
    private String tableName;
    /** Row Mapper */
    @Setter
    private Function<Result, T> mapper;
    /** Hbase cache size */
    @Setter
    private int cacheSize = 200;

    /** Hbase configuration */
    @Setter
    private Configuration configuration;

    /** Hbase table object */
    private Table table;
    /** Current reading scan */
    private int currentScan = -1;
    /** Results of the current scan */
    private Iterator<Result> results;

    /*
     * (non-Javadoc)
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(scans, "Scans must not be null");
        Assert.notNull(tableName, "tableName must not be null");
        Assert.notNull(mapper, "rowMapper must not be null");

        /* Hbase connection */
        Connection connection = ConnectionFactory.createConnection(configuration);
        this.table = connection.getTable(TableName.valueOf(tableName));
        // Configure scans
        scans.parallelStream().forEach(scan -> {
            scan.setCacheBlocks(false);
            scan.setCaching(cacheSize);
            scan.setMaxVersions(1);
            scan.setLoadColumnFamiliesOnDemand(false);
        });
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.batch.item.ItemReader#read()
     */
    @Override
    public T read() throws Exception {
        if (scans.isEmpty()) {
            return null;
        }

        if (currentScan == -1) {
            currentScan++;
            results = runScan(scans.get(currentScan));
        }

        T item = readFromDelegate();

        while (item == null) {
            currentScan++;
            if (currentScan >= scans.size()) {
                return null;
            }
            results = runScan(scans.get(currentScan));
            item = readFromDelegate();
        }

        return item;
    }

    /**
     * Execute a scan against a table object
     *
     * @param scan
     *            scan to run
     * @return result scanner
     */
    private Iterator<Result> runScan(@NonNull final Scan scan) {
        try {
            return this.table.getScanner(scan).iterator();
        } catch (final IOException e) {
            throw new IllegalStateException("Unable to get hbase scan iterator",
                    e);
        }
    }

    /**
     * Return the next item from resultScanner
     *
     * @return next item to read
     */
    private T readFromDelegate() {
        if (results.hasNext()) {
            return mapper.apply(results.next());
        }
        return null;
    }

}
