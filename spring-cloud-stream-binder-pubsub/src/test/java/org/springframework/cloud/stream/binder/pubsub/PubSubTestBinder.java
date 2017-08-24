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

import com.google.pubsub.v1.ProjectName;
import org.springframework.cloud.stream.binder.AbstractTestBinder;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubSupport;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.integration.codec.kryo.PojoCodec;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @author Vinicius Carvalho
 */
public class PubSubTestBinder extends
		AbstractTestBinder<PubSubMessageChannelBinder, ExtendedConsumerProperties<PubSubConsumerProperties>, ExtendedProducerProperties<PubSubProducerProperties>> {

	private String project;
	private PubSubSupport pubSubSupport;

	public PubSubTestBinder(String project, PubSubSupport pubSubSupport) {
		this.project = project;
		this.pubSubSupport = pubSubSupport;
		PubSubBinderConfigurationProperties config = new PubSubBinderConfigurationProperties();
		config.setProjectName(project);
		PubSubMessageChannelBinder binder = new PubSubMessageChannelBinder(
				config,
				new PubSubResourceManager(config,
						pubSubSupport.getSubscriptionAdminClient(),
						pubSubSupport.getTopicAdminClient()),
				new PubSubProvisioningProvider()
		);
		binder.setChannelProvider(pubSubSupport.getChannelProvider());
		binder.setCredentialsProvider(pubSubSupport.getCredentialsProvider());
		GenericApplicationContext context = new GenericApplicationContext();
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		scheduler.setPoolSize(1);
		scheduler.afterPropertiesSet();
		context.getBeanFactory().registerSingleton(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME, scheduler);
		context.refresh();
		binder.setApplicationContext(context);
		binder.setCodec(new PojoCodec());
		this.setBinder(binder);

	}

	@Override
	public void cleanup() {
		ProjectName projectName = ProjectName.newBuilder().setProject(project).build();
		pubSubSupport.getSubscriptionAdminClient()
				.listSubscriptions(projectName)
				.expandToFixedSizeCollection(Integer.MAX_VALUE)
				.getValues()
				.forEach(subscription -> {
					System.out.println("Deleting subscription: " + subscription.getName());
					pubSubSupport.getSubscriptionAdminClient()
							.deleteSubscription(subscription.getNameAsSubscriptionName());
				});
		pubSubSupport.getTopicAdminClient()
				.listTopics(projectName)
				.expandToFixedSizeCollection(Integer.MAX_VALUE)
				.getValues()
				.forEach(topic -> {
					System.out.println("Deleting topic: " + topic.getName());
					pubSubSupport.getTopicAdminClient().deleteTopic(topic.getNameAsTopicName());
				});
	}
}
