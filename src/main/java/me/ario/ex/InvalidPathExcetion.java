package me.ario.ex;

/**
 * the path is invalid
 */
public class InvalidPathExcetion extends RuntimeException {
    public InvalidPathExcetion(String message) {
        super(String.format("the path %s is INVALID",message));
    }
}
