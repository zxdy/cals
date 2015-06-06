package me.ario.ex;

import java.text.ParseException;

/**
 * Created by ario on 6/6/15.
 */
public class TimeParseException extends ParseException {

    public TimeParseException(String s, int errorOffset) {
        super(s, errorOffset);
    }
}
