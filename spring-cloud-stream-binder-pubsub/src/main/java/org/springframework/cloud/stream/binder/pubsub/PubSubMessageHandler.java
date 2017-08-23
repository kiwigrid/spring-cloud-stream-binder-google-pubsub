/*
 *  Copyright 2016 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.springframework.cloud.stream.binder.pubsub;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Vinicius Carvalho
 */
public abstract class PubSubMessageHandler extends AbstractMessageHandler implements Lifecycle {

	protected PubSubResourceManager resourceManager;
	protected ExtendedProducerProperties<PubSubProducerProperties> producerProperties;

	protected ObjectMapper mapper;

	protected ProducerDestination producerDestination;

	protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	protected volatile boolean running = false;

	public PubSubMessageHandler(PubSubResourceManager resourceManager,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties,
			ProducerDestination producerDestination)
	{
		this.resourceManager = resourceManager;
		this.producerProperties = producerProperties;
		this.mapper = new ObjectMapper();
		this.producerDestination = producerDestination;
	}

	protected PubSubMessage convert(Message<?> message) throws Exception {
		String encodedHeaders = encodeHeaders(message.getHeaders());
		String topic;
		if (producerProperties.isPartitioned()) {
			Integer partition = (Integer) message.getHeaders().get(BinderHeaders.PARTITION_HEADER);
			topic = producerDestination.getNameForPartition(partition);
		} else {
			topic = producerDestination.getName();
		}
		return new PubSubMessage(
				com.google.cloud.pubsub.Message
						.newBuilder(ByteArray.copyFrom((byte[]) message.getPayload()))
						.addAttribute(PubSubBinder.SCST_HEADERS, encodedHeaders).build(),
				topic);
	}

	protected String encodeHeaders(MessageHeaders headers) throws Exception {
		Map<String, Object> rawHeaders = new HashMap<>();
		for (String key : headers.keySet()) {
			rawHeaders.put(key, headers.get(key));
		}
		return mapper.writeValueAsString(rawHeaders);
	}

}
