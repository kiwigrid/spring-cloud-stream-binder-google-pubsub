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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.threeten.bp.Duration;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Andreas Berger
 */
public class PubSubMessageHandler extends AbstractMessageHandler implements Lifecycle {

	private static final Logger LOGGER = getLogger(PubSubMessageHandler.class);
	private final ConcurrentHashMap<String, Publisher> publisherCache = new ConcurrentHashMap<>();
	private final String projectName;

	private ExtendedProducerProperties<PubSubProducerProperties> producerProperties;
	private ObjectMapper mapper;
	private ProducerDestination producerDestination;
	private volatile boolean running = false;

	private CredentialsProvider credentialsProvider;
	private ChannelProvider channelProvider;

	public PubSubMessageHandler(
			String projectName,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties,
			ProducerDestination producerDestination)
	{
		this.projectName = projectName;
		this.producerProperties = producerProperties;
		this.mapper = new ObjectMapper();
		this.producerDestination = producerDestination;
	}

	@Override
	public void start() {
		running = true;
	}

	@Override
	public void stop() {
		for (Publisher publisher : publisherCache.values()) {
			try {
				publisher.shutdown();
			} catch (Exception e) {
				LOGGER.error("", e);
			}
		}
		publisherCache.clear();
		running = false;
	}

	@Override
	public boolean isRunning() {
		return running;
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		String topicName = getTopicName(message);

		String encodedHeaders = encodeHeaders(message.getHeaders());
		PubsubMessage pubsubMessage = PubsubMessage
				.newBuilder()
				.setData(ByteString.copyFrom((byte[]) message.getPayload()))
				.putAttributes(PubSubBinder.SCST_HEADERS, encodedHeaders)
				.build();

		Publisher publisher = getPublisher(topicName);
		publisher.publish(pubsubMessage);
	}

	private Publisher getPublisher(String topic) {
		return publisherCache.computeIfAbsent(topic, topicName -> {
			try {
				Publisher.Builder builder = Publisher
						.defaultBuilder(TopicName.create(projectName, topicName));
				if (credentialsProvider != null) {
					builder.setCredentialsProvider(credentialsProvider);
				}
				if (channelProvider != null) {
					builder.setChannelProvider(channelProvider);
				}
				PubSubProducerProperties extensionConf = producerProperties.getExtension();
				if (extensionConf.isBatchEnabled()) {
					BatchingSettings batchingSettings = BatchingSettings
							.newBuilder()
							.setIsEnabled(extensionConf.isBatchEnabled())
							.setElementCountThreshold(extensionConf.getBatchSize())
							.setRequestByteThreshold(1000L)// 1 kB
							.setDelayThreshold(Duration.ofMillis(extensionConf.getWindowSize()))
							.build();
					builder.setBatchingSettings(batchingSettings);
				}
				if (extensionConf.getConcurrency() != null) {
					builder.setExecutorProvider(InstantiatingExecutorProvider
							.newBuilder()
							.setExecutorThreadCount(extensionConf.getConcurrency())
							.build());

				}
				return builder.build();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		});
	}

	private String getTopicName(Message<?> message) {
		String topic;
		if (producerProperties.isPartitioned()) {
			Integer partition = (Integer) message.getHeaders().get(BinderHeaders.PARTITION_HEADER);
			topic = producerDestination.getNameForPartition(partition);
		} else {
			topic = producerDestination.getName();
		}
		return topic;
	}

	protected String encodeHeaders(MessageHeaders headers) throws Exception {
		Map<String, Object> rawHeaders = new HashMap<>();
		for (String key : headers.keySet()) {
			rawHeaders.put(key, headers.get(key));
		}
		return mapper.writeValueAsString(rawHeaders);
	}

	public PubSubMessageHandler setCredentialsProvider(CredentialsProvider credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
		return this;
	}

	public PubSubMessageHandler setChannelProvider(ChannelProvider channelProvider) {
		this.channelProvider = channelProvider;
		return this;
	}
}
