package de.tuberlin.pserver.crdt.exceptions;

public class NotUniqueException extends RuntimeException {
    public NotUniqueException(String message) {
        super(message);
    }
}
