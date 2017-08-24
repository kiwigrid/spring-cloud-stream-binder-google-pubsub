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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubExtendedBindingProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.integration.core.MessageProducer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<PubSubConsumerProperties>, ExtendedProducerProperties<PubSubProducerProperties>, PubSubProvisioningProvider>
		implements
		ExtendedPropertiesBinder<MessageChannel, PubSubConsumerProperties, PubSubProducerProperties> {

	private PubSubBinderConfigurationProperties pubSubBinderConfigurationProperties;

	private PubSubExtendedBindingProperties extendedBindingProperties = new PubSubExtendedBindingProperties();

	private PubSubResourceManager resourceManager;

	private CredentialsProvider credentialsProvider;

	private ChannelProvider channelProvider;

	public PubSubMessageChannelBinder(
			PubSubBinderConfigurationProperties pubSubBinderConfigurationProperties,
			PubSubResourceManager resourceManager,
			PubSubProvisioningProvider provisioningProvider
	)
	{
		super(true, new String[0], provisioningProvider);
		this.pubSubBinderConfigurationProperties = pubSubBinderConfigurationProperties;
		this.resourceManager = resourceManager;
	}

	protected void createProducerDestinationIfNecessary(ProducerDestination destination,
			ExtendedProducerProperties<PubSubProducerProperties> properties)
	{
		String prefix = properties.getExtension().getPrefix();
		if (properties.isPartitioned()) {
			for (int partitionIndex = 0; partitionIndex < properties.getPartitionCount(); partitionIndex++) {
				resourceManager.declareTopic(destination.getNameForPartition(partitionIndex), prefix, null);
			}
		} else {
			resourceManager.declareTopic(destination.getName(), prefix, null);
		}

	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties)
			throws Exception
	{
		createProducerDestinationIfNecessary(destination, producerProperties);
		PubSubMessageHandler handler = new PubSubMessageHandler(
				pubSubBinderConfigurationProperties.getProjectName(),
				producerProperties,
				destination);
		handler.setChannelProvider(channelProvider);
		handler.setCredentialsProvider(credentialsProvider);

		resourceManager.createRequiredMessageGroups(destination, producerProperties);

		return handler;
	}

	protected Subscription createConsumerDestinationIfNecessary(String topicName, String group,
			ExtendedConsumerProperties<PubSubConsumerProperties> properties)
	{
		boolean partitioned = properties.isPartitioned();
		Integer partitionIndex = null;
		if (partitioned) {
			partitionIndex = properties.getInstanceIndex();
		}
		Topic topic = resourceManager.declareTopic(topicName, properties.getExtension().getPrefix(), partitionIndex);
		Subscription subscription = resourceManager
				.declareSubscription(topic.getNameAsTopicName(), topic.getNameAsTopicName().getTopic(), group);
		return resourceManager.createSubscription(
				topic.getNameAsTopicName(),
				subscription.getNameAsSubscriptionName()
		);
	}

	@Override
	protected MessageProducer createConsumerEndpoint(
			ConsumerDestination destination,
			String group,
			ExtendedConsumerProperties<PubSubConsumerProperties> properties
	) throws Exception
	{
		Subscription subscription = createConsumerDestinationIfNecessary(destination.getName(), group, properties);
		return new PubSubMessageListener(subscription.getNameAsSubscriptionName(), properties)
				.setCredentialsProvider(credentialsProvider)
				.setChannelProvider(channelProvider);
	}

	@Override
	public PubSubConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public PubSubProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	public void setExtendedBindingProperties(PubSubExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	public PubSubMessageChannelBinder setCredentialsProvider(CredentialsProvider credentialsProvider) {
		this.credentialsProvider = credentialsProvider;
		return this;
	}

	public PubSubMessageChannelBinder setChannelProvider(ChannelProvider channelProvider) {
		this.channelProvider = channelProvider;
		return this;
	}
}
