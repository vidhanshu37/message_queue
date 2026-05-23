package com.mq.exception;

public class TopicNotFoundException extends RuntimeException {
    public TopicNotFoundException(String name) {
        super("Topic with name '" + name + "' not found.");
    }
}
