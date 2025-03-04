package com.teragrep.hbs_03;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;

public class StartDateFromMap implements OptionValue<Date> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartDateFromMap.class);

    private final Map<String, String> map;
    private final String key;

    public StartDateFromMap(final Map<String, String> map, final String key) {
        this.map = map;
        this.key = key;
    }

    @Override
    public Date value() {
        final Date start;
        if (map.containsKey(key)) {
            start = new ValidDateString(map.get(key)).date();
        } else {
            // default local date - 1 day
            start = Date.valueOf(LocalDate.now().minusDays(1));
            LOGGER.info("Using default start date <{}>", start);
        }
        return start;
    }
}
