package com.santander.kpv.config;

public final class CustomMQConstants {
    public static final String WMQ_HOST_NAME = "XMSC_WMQ_HOST_NAME";
    public static final String WMQ_PORT = "XMSC_WMQ_PORT";
    public static final String WMQ_CHANNEL = "XMSC_WMQ_CHANNEL";
    public static final String WMQ_CONNECTION_MODE = "XMSC_WMQ_CONNECTION_MODE";
    public static final String WMQ_QUEUE_MANAGER = "XMSC_WMQ_QUEUE_MANAGER";
    public static final String WMQ_APPLICATIONNAME = "XMSC_WMQ_APPNAME";
    public static final String USER_AUTHENTICATION_MQCSP = "XMSC_USER_AUTHENTICATION_MQCSP";
    public static final String USERID = "XMSC_USERID";
    public static final String PASSWORD = "XMSC_PASSWORD";

    public static final String WMQ_PROVIDER = "com.ibm.msg.client.wmq";

    private CustomMQConstants()throws IllegalAccessException {
        // Impedir instanciamento

    }
}
