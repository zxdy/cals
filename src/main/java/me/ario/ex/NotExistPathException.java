package me.ario.ex;

import java.io.File;

/**
 * the path does not exists
 */
public class NotExistPathException extends RuntimeException {
    public NotExistPathException(File message) {
        super(String.format("the path %s is not exists",message));
    }
}
