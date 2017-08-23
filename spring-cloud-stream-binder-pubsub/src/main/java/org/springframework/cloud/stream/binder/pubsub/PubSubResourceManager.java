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

import java.util.List;
import java.util.UUID;

import com.google.cloud.pubsub.*;
import org.slf4j.Logger;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.GroupedMessage;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
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

	private PubSub client;

	public PubSubResourceManager(PubSub client) {
		this.client = client;
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
					declareSubscription(destination.getNameForPartition(i), destination.getName(), requiredGroupName);
				}
			} else {
				declareSubscription(destination.getName(), destination.getName(), requiredGroupName);
			}
		}
	}

	/**
	 * Declares a subscription and returns its SubscriptionInfo
	 *
	 * @param topic the name of the topic
	 * @param name the name of the subscription
	 * @return the subscription info
	 */
	public SubscriptionInfo declareSubscription(String topic, String name, String group) {
		SubscriptionInfo subscription = null;
		String subscriptionName = createSubscriptionName(name, group);
		try {
			LOGGER.debug("Creating subscription: {} binding to topic : {}", subscriptionName, topic);
			subscription = client.create(SubscriptionInfo.of(topic, subscriptionName));
		} catch (PubSubException e) {
			if (e.getReason().equals(PubSubBinder.ALREADY_EXISTS)) {
				LOGGER.info("Subscription: {} already exists, reusing definition from remote server", subscriptionName);
				subscription = Subscription.of(topic, subscriptionName);
			}
		}
		return subscription;
	}

	public Subscription createSubscription(SubscriptionInfo subscriptionInfo) {
		Subscription subscription;
		try {
			subscription = client.create(subscriptionInfo);
		} catch (PubSubException e) {
			if (e.getReason().equals(PubSubBinder.ALREADY_EXISTS)) {
				subscription = client.getSubscription(subscriptionInfo.getName());
			} else {
				throw e;
			}
		}
		return subscription;
	}

	public PubSub.MessageConsumer createConsumer(SubscriptionInfo subscriptionInfo,
			PubSub.MessageProcessor processor)
	{
		return client.getSubscription(subscriptionInfo.getName()).pullAsync(processor);
	}

	public TopicInfo declareTopic(String name, String prefix, Integer partitionIndex) {
		TopicInfo topic = null;

		String topicName = createTopicName(name, prefix, partitionIndex);
		try {
			LOGGER.debug("Creating topic: {} ", topicName);
			topic = client.create(TopicInfo.of(topicName));
		} catch (PubSubException e) {
			if (e.getReason().equals(PubSubBinder.ALREADY_EXISTS)) {
				LOGGER.info("Topic: {} already exists, reusing definition from remote server", topicName);
				topic = Topic.of(topicName);
			}
		}

		return topic;
	}

	public String publishMessage(PubSubMessage pubSubMessage) {
		return client.publish(pubSubMessage.getTopic(), pubSubMessage.getMessage());
	}

	public List<String> publishMessages(GroupedMessage groupedMessage) {
		LOGGER.debug("Publishing {} messages to topic: {}",
				groupedMessage.getMessages().size(),
				groupedMessage.getTopic());
		return client.publish(groupedMessage.getTopic(), groupedMessage.getMessages());
	}

	public void deleteTopics(List<TopicInfo> topics) {
		for (TopicInfo t : topics) {
			client.deleteTopic(t.getName());
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
