package com.teragrep.hbs_03;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;

/**
 *
 */
public class MigrateDateRange implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateDateRange.class);

    private final Date start;
    private final Date end;
    private final DatabaseClient metaSQLClient;
    private final HBaseClient hbaseClient;

    public MigrateDateRange(final Date start, final Date end, DatabaseClient metaSQLClient, final HBaseClient hbaseClient) {
        this.start = start;
        this.end = end;
        this.metaSQLClient = metaSQLClient;
        this.hbaseClient = hbaseClient;
    }

    public void start() {
        final LogfileTable logfileTable = hbaseClient.logfile();
        logfileTable.create();
        LOGGER.info("Replication started from <{}> to <{}>", start, end);
        final LocalDate endDate = end.toLocalDate();
        long totalRows = 0;
        LocalDate rollingDay = start.toLocalDate();
        while (rollingDay.isBefore(endDate)) {
            final Date date = Date.valueOf(rollingDay);
            final int rows = metaSQLClient.migrateForDate(date, logfileTable);
            totalRows += rows;
            LOGGER.info("Processing date <{}> affected <{}> row(s)", date, rows);
            rollingDay = rollingDay.plusDays(1);
        }
        LOGGER.info("Replication finished on date <{}>, total rows <{}>", rollingDay, totalRows);
    }

    @Override
    public void close() {
        LOGGER.info("Closing clients");
        metaSQLClient.close();
        hbaseClient.close();
    }
}
