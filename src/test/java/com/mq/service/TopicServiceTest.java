package com.mq.service;

import com.mq.dto.request.CreateTopicRequest;
import com.mq.dto.response.TopicResponse;
import com.mq.exception.TopicAlreadyExistsException;
import com.mq.model.Topic;
import com.mq.repository.PartitionRepository;
import com.mq.repository.TopicRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TopicServiceTest {

    @Mock
    TopicRepository topicRepository;
    @Mock
    PartitionRepository partitionRepository;
    @InjectMocks
    TopicService topicService;

    @Test
    void createTopic_success() {
        CreateTopicRequest request = new CreateTopicRequest();
        request.setName("orders");
        request.setPartitionCount(3);

        when(topicRepository.existsByName("orders")).thenReturn(false);

        Topic savedTopic = new Topic("orders", 3);
        savedTopic.setId(1L);
        when(topicRepository.save(any(Topic.class))).thenReturn(savedTopic);

        TopicResponse response = topicService.createTopic(request);

        assertThat(response.getName()).isEqualTo("orders");
        assertThat(response.getPartitionCount()).isEqualTo(3);

        verify(partitionRepository).saveAll(argThat(partitions ->
                partitions.spliterator().estimateSize() == 3 ));
    }

    @Test
    void createTopic_duplicateName_throwsException() {
        CreateTopicRequest request = new CreateTopicRequest();
        request.setName("orders");
        request.setPartitionCount(1);

        when(topicRepository.existsByName("orders")).thenReturn(true);

        assertThatThrownBy(() -> topicService.createTopic(request))
                .isInstanceOf(TopicAlreadyExistsException.class)
                .hasMessageContaining("orders");
    }
}

