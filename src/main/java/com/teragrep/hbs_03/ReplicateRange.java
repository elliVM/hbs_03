package com.teragrep.hbs_03;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;

public class ReplicateRange {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicateRange.class);

    private final Date start;
    private final Date end;
    private final SQLDatabaseClient databaseClient;
    private final LogfileHBaseTable hBaseTable;

    public ReplicateRange(final Date start, final Date end, SQLDatabaseClient databaseClient,final LogfileHBaseTable hBaseTable) {
        this.start = start;
        this.end = end;
        this.databaseClient = databaseClient;
        this.hBaseTable = hBaseTable;
    }

    public void start() {
        LOGGER.info("Replication started from <{}> to <{}>", start, end);
        LocalDate endDate = end.toLocalDate();
        LocalDate rollingDay = start.toLocalDate();
        while (rollingDay.isBefore(endDate)) {
            final Date date = Date.valueOf(rollingDay);
            int rows = databaseClient.replicateDate(date, hBaseTable);

            LOGGER.info("Processing date <{}> affected <{}> row(s)", date, rows);

            rollingDay = rollingDay.plusDays(1);
        }
        LOGGER.info("Replication finished");
    }
}
