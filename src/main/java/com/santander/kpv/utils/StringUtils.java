package com.santander.kpv.utils;

public class StringUtils {
    private static final String STRING_SFH1 = """
            <?xml version="1.0"?>
            <requestMsg>
            <dse_operationName>SFH1</dse_operationName>
            <dse_formattedData>
            <kColl id="entryData">
            <field id="Usuario" value="MQKPVK" />
            <field id="NRDOCUM" value="99999990932"/>
            <field id="PENUMPE" value=""/>
            <field id="CODERRO" value=""/>
            <field id="MSGERRO" value=""/>
            </kColl>
            </dse_formattedData>
            <dse_processParameters>dse_operationName</dse_processParameters>
            </requestMsg>""";
    private static final String STRING_SFH8 = """
            <?xml version="1.0"?>
            <requestMsg>
            <dse_operationName>SHF8</dse_operationName>
            <dse_formattedData>
            <kColl id="entryData">
            <field id="Usuario" value="MQKPVK" />
            <field id="NRCPFCN" value="99999990932"/>
            </kColl>
            </dse_formattedData>
            <dse_processParameters>dse_operationName</dse_processParameters>
            </requestMsg>""";

    public static String getMensagem(String cpf, String sfh) {
        if (null == sfh){
            return "";
        }
        if (sfh.equals("sfh8")){
            return STRING_SFH8.replaceFirst("99999990932", cpf);
        } else {
            if (sfh.equals("sfh1")) {
                return STRING_SFH1.replaceFirst("99999990932", cpf);
            }
        }
        return "";
    }

    private StringUtils() {
    }
}
