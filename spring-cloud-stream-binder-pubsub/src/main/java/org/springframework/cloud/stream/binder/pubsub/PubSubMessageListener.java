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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.SubscriptionName;
import org.slf4j.Logger;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.threeten.bp.Duration;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageListener extends MessageProducerSupport {
	private static final Logger LOGGER = getLogger(PubSubMessageListener.class);
	private ObjectMapper mapper;
	private SubscriptionName subscriptionName;
	private Subscriber subscriber;
	private CredentialsProvider credentialsProvider;
	private ChannelProvider channelProvider;
	private ExtendedConsumerProperties<PubSubConsumerProperties> properties;

	public PubSubMessageListener(SubscriptionName subscriptionName, ExtendedConsumerProperties<PubSubConsumerProperties> properties) {
		this.properties = properties;
		this.mapper = new ObjectMapper();
		this.subscriptionName = subscriptionName;
	}

	@Override
	protected void doStart() {
		MessageReceiver receiver = (message, consumer) -> {
			sendMessage(getMessageBuilderFactory()
					.withPayload(message.getData().toByteArray())
					.copyHeaders(decodeAttributes(message.getAttributesMap())).build());
			consumer.ack();
		};

		Subscriber.Builder builder = Subscriber
				.defaultBuilder(subscriptionName, receiver)
				.setMaxAckExtensionPeriod(Duration.ofSeconds(properties.getExtension().getAckDeadlineSeconds()));
		if (credentialsProvider != null) {
			builder.setCredentialsProvider(credentialsProvider);
		}
		if (channelProvider != null) {
			builder.setChannelProvider(channelProvider);
		}
		subscriber = builder.build();
		subscriber.addListener(
				new Subscriber.Listener() {
					@Override
					public void failed(Subscriber.State from, Throwable failure) {
						// Handle failure. This is called when the Subscriber encountered a fatal error and is shutting down.
						LOGGER.error(from.name() + " failed", failure);
					}
				},
				MoreExecutors.directExecutor());
		subscriber.startAsync().awaitRunning();
	}

	private Map<String, Object> decodeAttributes(Map<String, String> attributes) {
		Map<String, Object> headers = new HashMap<>();
		if (attributes.get(PubSubBinder.SCST_HEADERS) != null) {
			try {
				//noinspection unchecked
				headers.putAll(mapper.readValue(attributes.get(PubSubBinder.SCST_HEADERS), Map.class));
			} catch (IOException e) {
				logger.error("Could not deserialize SCST_HEADERS");
			}

		}
		return headers;
	}

	@Override
	protected void doStop() {
		try {
			if (subscriber != null) {
				subscriber.stopAsync().awaitTerminated();
			}
		} catch (Exception e) {
			logger.error("Could not close pubsub message consumer");
		}
	}

	public PubSubMessageListener setCredentialsProvider(CredentialsProvider credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
		return this;
	}

	public PubSubMessageListener setChannelProvider(ChannelProvider channelProvider) {
		this.channelProvider = channelProvider;
		return this;
	}
}
