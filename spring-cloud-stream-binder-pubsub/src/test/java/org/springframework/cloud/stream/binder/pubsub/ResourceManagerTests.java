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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import org.junit.*;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionTestSupport;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubSupport;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;

/**
 * @author Vinicius Carvalho
 */
public class ResourceManagerTests {
	public static final String PROJECT_NAME = "test";

	@Rule
	public PubSubTestSupport rule = new PubSubTestSupport();

	private PubSubResourceManager resourceManager;
	private PubSubSupport pubSubSupport;

	@Before
	public void setup() {
		if (pubSubSupport == null) {
			pubSubSupport = rule.getResource();

		}
		PubSubBinderConfigurationProperties config = new PubSubBinderConfigurationProperties();
		config.setProjectName(PROJECT_NAME);
		if (resourceManager == null) {
			this.resourceManager = new PubSubResourceManager(
					config,
					pubSubSupport.getSubscriptionAdminClient(),
					pubSubSupport.getTopicAdminClient());
		}

	}

	@After
	public void tearDown() {
		ProjectName projectName = ProjectName.newBuilder().setProject(PROJECT_NAME).build();
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

	@Test
	public void createNonPartitionedSubscription() throws Exception {
		PubSubProducerProperties properties = new PubSubProducerProperties();

		ExtendedProducerProperties<PubSubProducerProperties> producerProperties = new ExtendedProducerProperties<>(
				properties);
		producerProperties.setRequiredGroups("hdfs", "average");
		producerProperties.getExtension().setPrefix("createNonPartitionedSubscription");
		Topic topic = resourceManager.declareTopic("test", properties.getPrefix(), null);
		resourceManager.createRequiredMessageGroups(new PubSubProvisioningProvider.PubSubProducerDestination(properties.getPrefix(),
				"test"), producerProperties);

		TopicName topicName = topic.getNameAsTopicName();
		Assert.assertNotNull(pubSubSupport.getTopicAdminClient().getTopic(topicName));
		pubSubSupport
				.getTopicAdminClient()
				.listTopicSubscriptions(topicName)
				.iterateAllAsSubscriptionName()
				.forEach(subscriptionName -> Assert.assertTrue(
						subscriptionName.getSubscription().startsWith("createNonPartitionedSubscription.test.")
				));
		pubSubSupport.getTopicAdminClient().deleteTopic(topicName);
	}

	@Test
	public void createPartitionedSubscription() throws Exception {
		PubSubProducerProperties properties = new PubSubProducerProperties();
		properties.setPrefix("createPartitionedSubscription");


		ExtendedProducerProperties<PubSubProducerProperties> producerProperties = new ExtendedProducerProperties<>(
				properties);
		producerProperties.setRequiredGroups("hdfs", "average");
		producerProperties.setPartitionCount(2);
		producerProperties.setPartitionKeyExtractorClass(PartitionTestSupport.class);

		List<Topic> topics = new ArrayList<>();
		for (int i = 0; i < producerProperties.getPartitionCount(); i++) {
			topics.add(resourceManager.declareTopic("test", properties.getPrefix(), i));
		}
		resourceManager.createRequiredMessageGroups(
				new PubSubProvisioningProvider.PubSubProducerDestination(properties.getPrefix(), "test"),
				producerProperties
		);


		for (Topic topic : topics) {
			AtomicInteger count = new AtomicInteger();
			pubSubSupport
					.getTopicAdminClient()
					.listTopicSubscriptions(topic.getNameAsTopicName())
					.iterateAllAsSubscriptionName()
					.forEach(subscriptionName -> {
						count.incrementAndGet();
						Assert.assertTrue(
								subscriptionName.getSubscription().startsWith("createPartitionedSubscription.test-")
						);
					});
			Assert.assertEquals(2, count.get());
		}
	}

}
