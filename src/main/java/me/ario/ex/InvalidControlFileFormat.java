package me.ario.ex;

/**
 * Created by ario on 6/6/15.
 */
public class InvalidControlFileFormat extends RuntimeException {
    public InvalidControlFileFormat(int Message) {
        super(String.format("Oops!The control file's format is not correct at line %d",Message));
    }
}
