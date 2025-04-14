package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TurboConfig {

    @JsonProperty("turbo_num")
    private int turboNum;

    @JsonProperty("redis_uri")
    private String redisUri;

    @JsonProperty("service_id")
    private String serviceId;

    @JsonProperty("redis_mode")
    private String redisMode;

}
