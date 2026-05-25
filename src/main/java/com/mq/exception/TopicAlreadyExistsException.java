package com.mq.exception;

public class TopicAlreadyExistsException extends RuntimeException {
    public TopicAlreadyExistsException(String name) {
        super("Topic with name '" + name + "' already exists.");
    }
}
