package com.santander.kpv.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MyRuntimeExceptionTest {

    @Test
    void testDefaultConstructor() {
        MyRuntimeException exception = new MyRuntimeException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testMessageConstructor() {
        String message = "This is a custom runtime exception message";
        MyRuntimeException exception = new MyRuntimeException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testMessageAndCauseConstructor() {
        String message = "This is a custom runtime exception message";
        Throwable cause = new RuntimeException("This is the cause");
        MyRuntimeException exception = new MyRuntimeException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testCauseConstructor() {
        Throwable cause = new RuntimeException("This is the cause");
        MyRuntimeException exception = new MyRuntimeException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.RuntimeException: This is the cause", exception.getMessage());
    }

    @Test
    void testFullConstructor() {
        String message = "This is a custom runtime exception message";
        Throwable cause = new RuntimeException("This is the cause");
        MyRuntimeException exception = new MyRuntimeException(message, cause, true, true);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals(true, exception.getSuppressed().length == 0);
    }
}
