package com.teragrep.hbs_03;

import java.sql.Date;
import java.util.Objects;
import java.util.regex.Pattern;

public class ValidDateString {
    private final Pattern pattern;
    private final String dateString;

    public ValidDateString(final String dateString) {
        this(dateString, Pattern.compile("\\d{4}-\\d{2}-\\d{2}"));
    }

    private ValidDateString(final String dateString, final Pattern pattern) {
        this.dateString = dateString;
        this.pattern = pattern;
    }

    public Date date() {
        if (valid()) {
            return Date.valueOf(dateString);
        }
        throw new IllegalArgumentException("Invalid date format <" + dateString + ">. Expected format YYYY-MM-DD");
    }

    private boolean valid() {
        return dateString != null && pattern.matcher(dateString).matches();
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null || getClass() != object.getClass()) return false;
        final ValidDateString validDateString = (ValidDateString) object;
        return Objects.equals(pattern, validDateString.pattern) && Objects.equals(dateString, validDateString.dateString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, dateString);
    }
}
