package com.cpeplatform.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CpeStatusDataDto {
    private String deviceId;
    private String status;
    private long timestamp;
}
