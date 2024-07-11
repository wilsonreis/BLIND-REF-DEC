package com.santander.kpv.config;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomMQConstantsTest {

    @Test
    void testPrivateConstructor() throws NoSuchMethodException {
        // Obter o construtor privado da classe
        Constructor<CustomMQConstants> constructor = CustomMQConstants.class.getDeclaredConstructor();

        // Verificar se o construtor é privado
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));

    }
}
