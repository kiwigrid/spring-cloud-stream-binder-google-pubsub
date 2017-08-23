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

import java.time.Duration;

import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.GroupedMessage;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubMessage;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.messaging.Message;
import reactor.core.Disposable;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;

/**
 * @author Vinicius Carvalho
 */
public class BatchingPubSubMessageHandler extends PubSubMessageHandler {

	private WorkQueueProcessor<PubSubMessage> processor;
	private Integer concurrency;
	private Disposable processorCancellation;

	public BatchingPubSubMessageHandler(
			PubSubResourceManager resourceManager,
			ExtendedProducerProperties<PubSubProducerProperties> producerProperties,
			ProducerDestination producerDestination)
	{
		super(resourceManager, producerProperties, producerDestination);
		this.processor = WorkQueueProcessor.share(true);
	}

	@Override
	protected void handleMessageInternal(Message<?> message) throws Exception {
		PubSubMessage pubSubMessage = convert(message);
		processor.onNext(pubSubMessage);
	}

	@Override
	public void start() {
		if (this.concurrency == null) {
			// We are assuming Brian Goetz (http://www.ibm.com/developerworks/java/library/j-jtp0730/index.html) cores * 1 + (1 + wait/service) and that http wait is taking 2x more than service
			this.concurrency = Runtime.getRuntime().availableProcessors() * 3;
		}
		this.processorCancellation = processor.groupBy(PubSubMessage::getTopic)
				.flatMap(group -> group
						.map(PubSubMessage::getMessage)
						.bufferTimeout(
								producerProperties.getExtension().getBatchSize(),
								Duration.ofMillis(producerProperties.getExtension().getWindowSize())
						)
						.map(messages -> new GroupedMessage(group.key(), messages)))
				.parallel(concurrency)
				.runOn(Schedulers.elastic()).doOnNext(groupedMessage -> {
					logger.info("Dispatching messages");
					resourceManager.publishMessages(groupedMessage);
				}).sequential().publishOn(Schedulers.elastic()).subscribe();
		running = true;
	}

	@SuppressWarnings("unused")
	public Integer getConcurrency() {
		return concurrency;
	}

	public void setConcurrency(Integer concurrency) {
		if (concurrency != null && concurrency > 0) {
			this.concurrency = Math.min(8 * Runtime.getRuntime().availableProcessors(), concurrency);
		}
	}

	@Override
	public void stop() {
		processorCancellation.dispose();
		running = false;
	}

	@Override
	public boolean isRunning() {
		return running;
	}
}
