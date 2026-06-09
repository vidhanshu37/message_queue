package com.mq.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BrokerInfo {
    private String brokerId;
    private String host;
    private int port;

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return brokerId + "@" + host + ":" + port;
    }
}
