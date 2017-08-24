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

import java.util.UUID;

import com.google.api.gax.grpc.GrpcApiException;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.*;
import io.grpc.Status;
import org.slf4j.Logger;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.util.StringUtils;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Vinicius Carvalho
 *
 * Utility class to manage PubSub resources. Deals with topic and subscription creation
 * and Future conversion
 *
 * Uses Spring Cloud Stream properties to determine the need to create partitions and
 * consumer groups
 */
public class PubSubResourceManager {

	private static final Logger LOGGER = getLogger(PubSubResourceManager.class);

	private PubSubBinderConfigurationProperties configurationProperties;

	private SubscriptionAdminClient subscriptionAdminClient;

	private TopicAdminClient topicAdminClient;

	public PubSubResourceManager(
			PubSubBinderConfigurationProperties configurationProperties,
			SubscriptionAdminClient subscriptionAdminClient,
			TopicAdminClient topicAdminClient
	)
	{
		this.configurationProperties = configurationProperties;
		this.subscriptionAdminClient = subscriptionAdminClient;
		this.topicAdminClient = topicAdminClient;
	}

	public static String applyPrefix(String prefix, String name) {
		if (StringUtils.isEmpty(prefix)) {
			return name;
		}
		return prefix + PubSubBinder.GROUP_INDEX_DELIMITER + name;
	}

	public void createRequiredMessageGroups(ProducerDestination destination,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties)
	{
		for (String requiredGroupName : producerProperties.getRequiredGroups()) {
			if (producerProperties.isPartitioned()) {
				for (int i = 0; i < producerProperties.getPartitionCount(); i++) {
					String name = destination.getNameForPartition(i);
					declareSubscription(name, name, requiredGroupName);
				}
			} else {
				declareSubscription(destination.getName(), destination.getName(), requiredGroupName);
			}
		}
	}

	public Subscription declareSubscription(String topicName, String subscriptionName, String group) {
		return declareSubscription(TopicName.create(configurationProperties.getProjectName(), topicName),
				subscriptionName,
				group);
	}

	/**
	 * Declares a subscription and returns its SubscriptionInfo
	 *
	 * @param topicName the name of the topic
	 * @param subscriptionName the name of the subscription
	 * @return the subscription info
	 */
	public Subscription declareSubscription(TopicName topicName, String subscriptionName, String group) {
		SubscriptionName name = SubscriptionName.create(
				topicName.getProject(),
				createSubscriptionName(subscriptionName, group)
		);
		return createSubscription(topicName, name);
	}

	public Subscription createSubscription(TopicName topicName, SubscriptionName subscriptionName) {
		try {
			return subscriptionAdminClient.createSubscription(
					subscriptionName,
					topicName,
					PushConfig.getDefaultInstance(),
					0);
		} catch (GrpcApiException e) {
			if (e.getStatusCode().getCode() == Status.Code.ALREADY_EXISTS) {
				LOGGER.info("subscription: {} already exists, reusing definition from remote server", subscriptionName);
				return Subscription
						.newBuilder()
						.setNameWithSubscriptionName(subscriptionName)
						.setTopicWithTopicNameOneof(TopicNameOneof.from(topicName))
						.build();
			}
			throw e;
		}
	}

	public Topic declareTopic(String name, String prefix, Integer partitionIndex) {
		TopicName topicName = TopicName.create(
				configurationProperties.getProjectName(),
				createTopicName(name, prefix, partitionIndex)
		);
		try {
			LOGGER.info("try creating topic: {} ", topicName);
			return topicAdminClient.createTopic(topicName);
		} catch (GrpcApiException e) {
			if (e.getStatusCode().getCode() == Status.Code.ALREADY_EXISTS) {
				LOGGER.info("topic: {} already exists, reusing definition from remote server", topicName);
				return Topic
						.newBuilder()
						.setNameWithTopicName(topicName)
						.build();
			}
			throw e;
		}
	}

	public String createTopicName(String name, String prefix, Integer partitionIndex) {
		StringBuilder buffer = new StringBuilder();
		buffer.append(applyPrefix(prefix, name));

		if (partitionIndex != null) {
			buffer.append("-").append(partitionIndex);
		}
		return buffer.toString();
	}

	private String createSubscriptionName(String name, String group) {
		boolean anonymousConsumer = !StringUtils.hasText(group);
		StringBuilder buffer = new StringBuilder();
		if (anonymousConsumer) {
			buffer.append(groupedName(name, UUID.randomUUID().toString()));
		} else {
			buffer.append(groupedName(name, group));
		}
		return buffer.toString();
	}

	public final String groupedName(String name, String group) {
		return name + PubSubBinder.GROUP_INDEX_DELIMITER + (StringUtils.hasText(group) ? group : "default");
	}

}
