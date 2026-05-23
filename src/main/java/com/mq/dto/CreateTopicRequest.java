package com.mq.dto;

import jakarta.validation.constraints.*;
import lombok.Data;

@Data
public class CreateTopicRequest {

    @NotBlank(message = "Topic name must not be blank")
    @Pattern(regexp = "^[a-zA-Z0-9_-]+$", message = "Topic name can only contain letters, digits, hyphens, underscores")
    private String name;

    @Min(value = 1, message = "Must have at least 1 partition")
    @Max(value = 64, message = "Cannot exceed 64 partitions")
    private int partitionCount = 1;
}