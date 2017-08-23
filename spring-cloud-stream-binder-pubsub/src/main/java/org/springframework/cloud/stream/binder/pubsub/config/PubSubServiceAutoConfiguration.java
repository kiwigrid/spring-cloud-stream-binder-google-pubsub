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

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.util.Base64;
import com.google.cloud.AuthCredentials;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.pubsub.PubSubMessageChannelBinder;
import org.springframework.cloud.stream.binder.pubsub.PubSubProvisioningProvider;
import org.springframework.cloud.stream.binder.pubsub.PubSubResourceManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.integration.codec.Codec;

/**
 * @author Vinicius Carvalho
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@ConditionalOnClass(PubSub.class)
@EnableConfigurationProperties({ PubSubBinderConfigurationProperties.class,
		PubSubExtendedBindingProperties.class })
public class PubSubServiceAutoConfiguration {

	@SuppressWarnings("SpringJavaAutowiringInspection")
	@Autowired
	private Codec codec;

	@Autowired
	private PubSubExtendedBindingProperties pubSubExtendedBindingProperties;

	@Bean
	public PubSubResourceManager pubSubResourceManager(PubSub pubSub) {
		return new PubSubResourceManager(pubSub);
	}

	@Bean
	public PubSubProvisioningProvider pubSubProvisioningProvider() {
		return new PubSubProvisioningProvider();
	}

	@Bean
	public PubSubMessageChannelBinder binder(PubSubResourceManager resourceManager, PubSubProvisioningProvider pubSubProvisioningProvider)
			throws Exception
	{
		PubSubMessageChannelBinder binder = new PubSubMessageChannelBinder(resourceManager, pubSubProvisioningProvider);
		binder.setExtendedBindingProperties(this.pubSubExtendedBindingProperties);
		binder.setCodec(codec);
		return binder;
	}

	@ConditionalOnMissingBean(PubSub.class)
	@Configuration
	public static class PubSubConfiguration {

		@SuppressWarnings("SpringJavaAutowiringInspection")
		@Autowired
		private PubSubBinderConfigurationProperties pubSubBinderConfigurationProperties;

		@Value("${vcap.services.google.credentials.email:}")
		private String email;

		@Value("${vcap.services.google.credentials.privateKey:}")
		private String privateKey;

		@Bean
		@Profile("!cloud")
		public PubSub pubSubNoCloud() {
			return PubSubOptions
					.newBuilder()
					.setProjectId(pubSubBinderConfigurationProperties.getProjectName())
					.build()
					.getService();
		}

		@Bean
		@Profile("cloud")
		public PubSub pubSubCloud(Environment environment) throws Exception {
			String vcapServicesEnv = environment.getProperty("VCAP_SERVICES");
			JsonParser parser = JsonParserFactory.getJsonParser();
			Map<String, Object> services = parser.parseMap(vcapServicesEnv);
			//noinspection unchecked
			List<Map<String, Object>> googlePubsub = (List<Map<String, Object>>) services.get("google-pubsub");
			//noinspection unchecked
			Map<String, Object> credentials = (Map<String, Object>) googlePubsub.get(0).get("credentials");
			String privateKeyData = (String) credentials.get("PrivateKeyData");
			GoogleCredential googleCredential = GoogleCredential.fromStream(new ByteArrayInputStream(Base64.decodeBase64(
					privateKeyData)));

			return PubSubOptions
					.newBuilder()
					.setAuthCredentials(
							AuthCredentials.createFor(
									googleCredential.getServiceAccountId(),
									googleCredential.getServiceAccountPrivateKey())
					)
					.setProjectId(pubSubBinderConfigurationProperties.getProjectName())
					.build()
					.getService();
		}
	}

}
