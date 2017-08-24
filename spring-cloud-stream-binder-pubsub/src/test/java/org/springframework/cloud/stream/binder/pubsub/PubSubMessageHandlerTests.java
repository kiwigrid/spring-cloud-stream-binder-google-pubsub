package org.springframework.cloud.stream.binder.pubsub;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubSupport;
import org.springframework.cloud.stream.binder.test.junit.pubsub.PubSubTestSupport;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Vinicius Carvalho
 */
public class PubSubMessageHandlerTests {

	public static final String PROJECT_NAME = "test";
	@Rule
	public PubSubTestSupport rule = new PubSubTestSupport();

	private PubSubResourceManager resourceManager;
	private PubSubSupport pubSubSupport;

	@Before
	public void setup() throws Exception {
		pubSubSupport = rule.getResource();
		PubSubBinderConfigurationProperties config = new PubSubBinderConfigurationProperties();
		config.setProjectName(PROJECT_NAME);
		this.resourceManager = new PubSubResourceManager(
				config,
				pubSubSupport.getSubscriptionAdminClient(),
				pubSubSupport.getTopicAdminClient());
	}

	@Test
	public void consumeMessages() throws Exception {
		int messageCount = 2000;
		final AtomicInteger counter = new AtomicInteger(0);
		CountDownLatch latch = new CountDownLatch(messageCount);
		String baseTopicName = "pubsub-test";
		ExtendedProducerProperties<PubSubProducerProperties> extendedProducerProperties = new ExtendedProducerProperties<>(
				new PubSubProducerProperties());
		Topic topic = resourceManager.declareTopic(baseTopicName, "", null);
		Subscription subscription = resourceManager.declareSubscription(topic.getNameAsTopicName(),
				"test-subscription",
				"");
		PubSubMessageHandler messageHandler = new PubSubMessageHandler(
				PROJECT_NAME,
				extendedProducerProperties,
				new PubSubProvisioningProvider.PubSubProducerDestination("", baseTopicName)
		);
		Subscriber.Builder builder = Subscriber
				.defaultBuilder(subscription.getNameAsSubscriptionName(), (message, consumer) -> {
					counter.incrementAndGet();
					latch.countDown();
					consumer.ack();
				});
		if (pubSubSupport.getCredentialsProvider() != null) {
			builder.setCredentialsProvider(pubSubSupport.getCredentialsProvider());
			messageHandler.setCredentialsProvider(pubSubSupport.getCredentialsProvider());
		}
		if (pubSubSupport.getChannelProvider() != null) {
			builder.setChannelProvider(pubSubSupport.getChannelProvider());
			messageHandler.setChannelProvider(pubSubSupport.getChannelProvider());
		}
		messageHandler.start();

		Subscriber subscriber = builder.build();
		subscriber.startAsync().awaitRunning();
		for (int j = 0; j < messageCount; j++) {
			String payload = "foo-" + j;
			messageHandler.handleMessage(MessageBuilder.withPayload(payload.getBytes()).build());
		}
		latch.await(20, TimeUnit.SECONDS);
		subscriber.stopAsync().awaitTerminated(20, TimeUnit.SECONDS);
		Assert.assertEquals(messageCount, counter.get());
	}

}
