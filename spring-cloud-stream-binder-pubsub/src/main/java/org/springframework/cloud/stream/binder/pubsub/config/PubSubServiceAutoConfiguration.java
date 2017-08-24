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

package org.springframework.cloud.stream.binder.pubsub.config;

import java.io.IOException;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.pubsub.PubSubMessageChannelBinder;
import org.springframework.cloud.stream.binder.pubsub.PubSubProvisioningProvider;
import org.springframework.cloud.stream.binder.pubsub.PubSubResourceManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.codec.Codec;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@ConditionalOnClass({ TopicAdminClient.class, SubscriptionAdminClient.class })
@EnableConfigurationProperties({ PubSubBinderConfigurationProperties.class, PubSubExtendedBindingProperties.class })
public class PubSubServiceAutoConfiguration {

	@SuppressWarnings("SpringJavaAutowiringInspection")
	@Autowired
	private Codec codec;

	@Autowired
	private PubSubExtendedBindingProperties pubSubExtendedBindingProperties;

	@SuppressWarnings("SpringJavaAutowiringInspection")
	@Autowired
	private PubSubBinderConfigurationProperties pubSubBinderConfigurationProperties;

	@ConditionalOnMissingBean(SubscriptionAdminClient.class)
	@Bean
	public SubscriptionAdminClient subscriptionAdminClient() throws IOException {
		return SubscriptionAdminClient.create();
	}

	@ConditionalOnMissingBean(TopicAdminClient.class)
	@Bean
	public TopicAdminClient topicAdminClient() throws IOException {
		return TopicAdminClient.create();
	}

	@Bean
	public PubSubResourceManager pubSubResourceManager(
			SubscriptionAdminClient subscriptionAdminClient,
			TopicAdminClient topicAdminClient
	)
	{
		return new PubSubResourceManager(
				pubSubBinderConfigurationProperties,
				subscriptionAdminClient,
				topicAdminClient);
	}

	@Bean
	public PubSubProvisioningProvider pubSubProvisioningProvider() {
		return new PubSubProvisioningProvider();
	}

	@Bean
	public PubSubMessageChannelBinder binder(PubSubResourceManager resourceManager, PubSubProvisioningProvider pubSubProvisioningProvider)
			throws Exception
	{
		PubSubMessageChannelBinder binder = new PubSubMessageChannelBinder(
				pubSubBinderConfigurationProperties,
				resourceManager,
				pubSubProvisioningProvider
		);
		binder.setExtendedBindingProperties(this.pubSubExtendedBindingProperties);
		binder.setCodec(codec);
		return binder;
	}

}
