package com.teragrep.hbs_03;

public class HbsRuntimeException extends RuntimeException {
    public HbsRuntimeException(final String message, final Throwable cause) {
        super(message + " (caused by: " + cause.getClass().getSimpleName() + ": " + cause.getMessage() + ")", cause);
    }
}
