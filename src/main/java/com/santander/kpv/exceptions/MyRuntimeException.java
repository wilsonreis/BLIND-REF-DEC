package com.santander.kpv.exceptions;

public class MyRuntimeException extends RuntimeException {

    // Construtor padrão
    public MyRuntimeException() {
        super();
    }

    // Construtor que aceita uma mensagem
    public MyRuntimeException(String message) {
        super(message);
    }

    // Construtor que aceita uma mensagem e uma causa
    public MyRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    // Construtor que aceita uma causa
    public MyRuntimeException(Throwable cause) {
        super(cause);
    }

    // Construtor que aceita uma mensagem, uma causa, supressão ativada ou desativada, e se a pilha de chamada é gravável
    protected MyRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
