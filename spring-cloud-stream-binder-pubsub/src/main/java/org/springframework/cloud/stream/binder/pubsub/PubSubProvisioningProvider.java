package org.springframework.cloud.stream.binder.pubsub;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubConsumerProperties;
import org.springframework.cloud.stream.binder.pubsub.config.PubSubProducerProperties;
import org.springframework.cloud.stream.binder.pubsub.support.PubSubBinder;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.util.StringUtils;

/**
 * @author Andreas Berger
 */
public class PubSubProvisioningProvider implements
		ProvisioningProvider<ExtendedConsumerProperties<PubSubConsumerProperties>, ExtendedProducerProperties<PubSubProducerProperties>> {
	@Override
	public ProducerDestination provisionProducerDestination(String name, ExtendedProducerProperties<PubSubProducerProperties> properties)
			throws ProvisioningException
	{
		return new PubSubProducerDestination(properties.getExtension().getPrefix(), name);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group, ExtendedConsumerProperties<PubSubConsumerProperties> properties)
			throws ProvisioningException
	{
		return () -> name;
	}

	public static class PubSubProducerDestination implements ProducerDestination {
		private final String fullName;

		public PubSubProducerDestination(String prefix, String name) {
			if (StringUtils.isEmpty(prefix)) {
				fullName = name;
			} else {
				fullName = prefix + PubSubBinder.GROUP_INDEX_DELIMITER + name;
			}

		}

		@Override
		public String getName() {
			return fullName;
		}

		@Override
		public String getNameForPartition(int partition) {
			return fullName + "-" + partition;
		}
	}
}
