package com.santander.kpv.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MyExceptionTest {

    @Test
    void testDefaultConstructor() {
        MyException exception = new MyException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testMessageConstructor() {
        String message = "This is a custom exception message";
        MyException exception = new MyException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    void testMessageAndCauseConstructor() {
        String message = "This is a custom exception message";
        Throwable cause = new RuntimeException("This is the cause");
        MyException exception = new MyException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    void testCauseConstructor() {
        Throwable cause = new RuntimeException("This is the cause");
        MyException exception = new MyException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.RuntimeException: This is the cause", exception.getMessage());
    }

    @Test
    void testFullConstructor() {
        String message = "This is a custom exception message";
        Throwable cause = new RuntimeException("This is the cause");
        MyException exception = new MyException(message, cause, true, true);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
        assertEquals(true, exception.getSuppressed().length == 0);
    }
}
