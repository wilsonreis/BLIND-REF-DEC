package com.santander.kpv.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;


class StringUtilsTest {

    @Test
    void testGetMensagemForSFH8() {
        StringUtils stringUtils = new StringUtils();
        String cpf = "12345678901";
        String sfh = "sfh8";

        String expected = """
                <?xml version="1.0"?>
                <requestMsg>
                <dse_operationName>SHF8</dse_operationName>
                <dse_formattedData>
                <kColl id="entryData">
                <field id="Usuario" value="MQKPVK" />
                <field id="NRCPFCN" value="12345678901"/>
                </kColl>
                </dse_formattedData>
                <dse_processParameters>dse_operationName</dse_processParameters>
                </requestMsg>""";

        String result = stringUtils.getMensagem(cpf, sfh);
        assertEquals(expected, result);
    }

    @Test
    void testGetMensagemForSFH1() {
        StringUtils stringUtils = new StringUtils();
        String cpf = "12345678901";
        String sfh = "sfh1";

        String expected = """
                <?xml version="1.0"?>
                <requestMsg>
                <dse_operationName>SFH1</dse_operationName>
                <dse_formattedData>
                <kColl id="entryData">
                <field id="Usuario" value="MQKPVK" />
                <field id="NRDOCUM" value="12345678901"/>
                <field id="PENUMPE" value=""/>
                <field id="CODERRO" value=""/>
                <field id="MSGERRO" value=""/>
                </kColl>
                </dse_formattedData>
                <dse_processParameters>dse_operationName</dse_processParameters>
                </requestMsg>""";

        String result = stringUtils.getMensagem(cpf, sfh);
        assertEquals(expected, result);
    }

    @Test
    void testGetMensagemForNullSFH() {
        StringUtils stringUtils = new StringUtils();
        String cpf = "12345678901";
        String sfh = null;

        String result = stringUtils.getMensagem(cpf, sfh);
        assertEquals("", result);
    }

    @Test
    void testGetMensagemForInvalidSFH() {
        StringUtils stringUtils = new StringUtils();
        String cpf = "12345678901";
        String sfh = "invalid";

        String result = stringUtils.getMensagem(cpf, sfh);
        assertEquals("", result);
    }
}
