package com.santander.kpv.fake;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Fake {
    private String userName;
    private String systemId;
    private String remoteAddr;
    private String loginTimestamp;

}