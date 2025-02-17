package com.teragrep.hbs_03;

import com.teragrep.cnf_01.Configuration;
import com.teragrep.cnf_01.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;

public class MigrateDateRangeFactory implements Factory<MigrateDateRange> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateDateRangeFactory.class);

    private final Configuration config;

    public MigrateDateRangeFactory(Configuration config) {
        this.config = config;
    }

    @Override
    public MigrateDateRange object() {
        final Date startDate;
        final Date endDate;
        final DatabaseClient databaseClient = new DatabaseClientFactory(config).object();
        final HBaseClient hbaseClient = new HBaseClientFactory(config).object();

        try {
            final Map<String, String> map = config.asMap();
            validate(map);
            startDate = new ValidDateString(map.get("migration.start")).date();
            // defaults to system local date
            // TODO: this could be forced to certain timezone for example UTC with LocalDate.now(ZoneOffset.UTC)
            endDate = new ValidDateString(map.getOrDefault("migration.end", LocalDate.now().toString())).date();
        } catch (final ConfigurationException e) {
            throw new HbsRuntimeException("Error getting configuration", e);
        }

        return new MigrateDateRange(startDate, endDate, databaseClient, hbaseClient);
    }

    private void validate(final Map<String, String> map) {
        if (!map.containsKey("migration.start")) {
            LOGGER.info("<migration.start> option missing ");
            throw new IllegalArgumentException("<migration.start> option missing");
        }
        if (!map.containsKey("migration.end")) {
            LOGGER.info("<migration.end> option missing, using system-dependent date for today");
        }
    }
}
