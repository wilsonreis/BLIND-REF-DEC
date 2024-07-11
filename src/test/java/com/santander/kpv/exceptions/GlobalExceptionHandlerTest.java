package com.santander.kpv.exceptions;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.Locale;

@ExtendWith(MockitoExtension.class)
@WebMvcTest(GlobalExceptionHandler.class)
class GlobalExceptionHandlerTest {

    @Mock
    private MessageSource messageSource;

    @InjectMocks
    private GlobalExceptionHandler globalExceptionHandler;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp(WebApplicationContext webApplicationContext) {
        mockMvc = MockMvcBuilders.standaloneSetup(new TestController())
                .setControllerAdvice(globalExceptionHandler)
                .build();
    }

    @Test
    void testHandleQueryTimeoutException() throws Exception {
        mockMvc.perform(get("/test/queryTimeout"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$[0].mensagemUsuario").value("Excessão imprevista (QueryTimeoutException)"));
    }

    @Test
    void testHandleMyRuntimeException() throws Exception {
        mockMvc.perform(get("/test/myRuntime"))
                .andExpect(status().isGatewayTimeout())
                .andExpect(jsonPath("$[0].mensagemUsuario").value("Excessão imprevista/Gateway Timeout (Exception)"));
    }

    // Controller de teste para simular as exceções
    @RestController
    public static class TestController {

        @GetMapping("/test/queryTimeout")
        public void queryTimeout() {
            throw new QueryTimeoutException("Query timeout");
        }

        @GetMapping("/test/myRuntime")
        public void myRuntime() {
            throw new MyRuntimeException("My runtime exception");
        }

        @GetMapping("/test/emptyResult")
        public void emptyResult() {
            throw new EmptyResultDataAccessException(1);
        }

        @GetMapping("/test/dataIntegrityViolation")
        public void dataIntegrityViolation() {
            throw new DataIntegrityViolationException("duplicate key value violates unique constraint");
        }
    }
}
