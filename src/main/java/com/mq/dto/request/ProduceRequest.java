package com.mq.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;

@Data
public class ProduceRequest {
    @NotBlank(message = "Topic name must not be blank")
    private String topicName;

    // Key is optional - if null, then round-robin selection is used
    // if provided, then use hash(key) % partitionCount determines partition
    private String key;

    @NotNull(message = "Value must not be null")
    @Size(min = 1, max = 1048576, message = " Message value must be between 1 byte and 1 MB")
    private String value;

}
