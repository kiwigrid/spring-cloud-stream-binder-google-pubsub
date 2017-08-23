package org.springframework.cloud.stream.binder.pubsub;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.TopicInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageHandlerTests {

	@Rule
	public PubSubTestSupport rule = new PubSubTestSupport();

	private PubSubResourceManager resourceManager;

	@Before
	public void setup() throws Exception {
		this.resourceManager = new PubSubResourceManager(rule.getResource());
	}

	@Test
	public void consumeMessages() throws Exception {

		int messageCount = 2000;
		final AtomicInteger counter = new AtomicInteger(0);
		CountDownLatch latch = new CountDownLatch(messageCount);
		String baseTopicName = "pubsub-test";
		ExtendedProducerProperties<PubSubProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(
				new PubSubProducerProperties());
		TopicInfo topicInfo = resourceManager.declareTopic(baseTopicName, "", null);
		SubscriptionInfo subscriptionInfo = resourceManager.declareSubscription(topicInfo.getName(),
				"test-subscription",
				"");
		PubSubMessageHandler messageHandler = new BatchingPubSubMessageHandler(
				resourceManager,
				extendedProducerProperties,
				new PubSubProvisioningProvider.PubSubProducerDestination(
						"",
						baseTopicName
				)
		);
		messageHandler.start();
		resourceManager.createConsumer(subscriptionInfo, message -> {
			counter.incrementAndGet();
			latch.countDown();
		});
		for (int j = 0; j < messageCount; j++) {
			String payload = "foo-" + j;
			messageHandler.handleMessage(MessageBuilder.withPayload(payload.getBytes()).build());
		}
		latch.await(20, TimeUnit.SECONDS);
		Assert.assertEquals(messageCount, counter.get());
	}

}
