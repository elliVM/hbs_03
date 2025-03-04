package com.teragrep.hbs_03;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;

public final class EndDateFromMap implements OptionValue<Date> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndDateFromMap.class);

    private final Map<String, String> map;
    private final String key;

    public EndDateFromMap(final Map<String, String> map,final String key) {
        this.map = map;
        this.key = key;
    }

    @Override
    public Date value() {
        final Date end;
        if (map.containsKey(key)) {
            end = new ValidDateString(map.get(key)).date();
        }
        else {
            // default local date
            end = Date.valueOf(LocalDate.now());
            LOGGER.info("Using default end date <{}>", end);
        }
        return end;
    }
}
