package io.onurb.tools.qcsv;

/**
 * Base exception for all errors thrown during the execution.
 */
public class QException extends Exception {

    public QException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public QException(String msg) {
        super(msg);
    }
}
