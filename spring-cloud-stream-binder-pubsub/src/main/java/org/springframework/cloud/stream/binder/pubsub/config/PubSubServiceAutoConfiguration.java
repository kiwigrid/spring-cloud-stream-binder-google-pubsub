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
import java.net.URI;
import java.util.List;
import java.util.Map;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelProvider;
import com.google.api.gax.grpc.FixedChannelProvider;
import com.google.api.gax.grpc.GrpcTransportProvider;
import com.google.api.gax.rpc.TransportProvider;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

	@Autowired(required = false)
	private CredentialsProvider credentialsProvider;

	@Autowired(required = false)
	private TransportProvider transportProvider;

	@Autowired(required = false)
	private ChannelProvider channelProvider;

	@SuppressWarnings("SpringJavaAutowiringInspection")
	@Autowired
	private PubSubBinderConfigurationProperties pubSubBinderConfigurationProperties;

	@ConditionalOnMissingBean(SubscriptionAdminClient.class)
	@Bean
	public SubscriptionAdminClient subscriptionAdminClient() throws IOException {
		SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.defaultBuilder();
		if (credentialsProvider != null) {
			builder.setCredentialsProvider(credentialsProvider);
		}
		if (transportProvider != null) {
			builder.setTransportProvider(transportProvider);
		}
		return SubscriptionAdminClient.create(builder.build());

	}

	@ConditionalOnMissingBean(TopicAdminClient.class)
	@Bean
	public TopicAdminClient topicAdminClient() throws IOException {
		TopicAdminSettings.Builder builder = TopicAdminSettings.defaultBuilder();
		if (credentialsProvider != null) {
			builder.setCredentialsProvider(credentialsProvider);
		}
		if (transportProvider != null) {
			builder.setTransportProvider(transportProvider);
		}
		return TopicAdminClient.create(builder.build());
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
		binder.setCredentialsProvider(credentialsProvider);
		binder.setChannelProvider(channelProvider);
		return binder;
	}

	@Configuration
	@ConditionalOnProperty(name = "PUBSUB_EMULATOR_HOST", relaxedNames = false)
	public static class EmulatorConfiguration {

		@Value("${PUBSUB_EMULATOR_HOST}")
		private String emulatorHost;

		@ConditionalOnMissingBean(CredentialsProvider.class)
		@Bean
		public CredentialsProvider fixedCredentialsProvider()
		{
			// ignore credentials if we have a local setup with emulator
			return FixedCredentialsProvider.create(new OAuth2Credentials(null) {
				@Override
				public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
					return null;
				}
			});
		}

		@ConditionalOnMissingBean(ChannelProvider.class)
		@Bean
		public ChannelProvider channelProvider() {
			ManagedChannel channel = ManagedChannelBuilder
					.forTarget(emulatorHost)
					.usePlaintext(true)
					.build();

			return FixedChannelProvider.create(channel);
		}

		@ConditionalOnMissingBean(TransportProvider.class)
		@Bean
		public TransportProvider transportProvider(ChannelProvider channelProvider) {
			return GrpcTransportProvider
					.newBuilder()
					.setChannelProvider(channelProvider)
					.build();
		}
	}

}
