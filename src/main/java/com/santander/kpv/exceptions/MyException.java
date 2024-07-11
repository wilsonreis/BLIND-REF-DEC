package com.santander.kpv.exceptions;


public class MyException extends Exception {

    // Construtor padrão
    public MyException() {
        super();
    }

    // Construtor que aceita uma mensagem
    public MyException(String message) {
        super(message);
    }

    // Construtor que aceita uma mensagem e uma causa
    public MyException(String message, Throwable cause) {
        super(message, cause);
    }

    // Construtor que aceita uma causa
    public MyException(Throwable cause) {
        super(cause);
    }

    // Construtor que aceita uma mensagem, uma causa, supressão ativada ou desativada, e se a pilha de chamada é gravável
    protected MyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
