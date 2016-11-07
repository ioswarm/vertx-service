package example.sharding;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;

public class ExampleShard extends AbstractVerticle {

	private String id;
	private MessageConsumer<Object> consumer;
	
	public ExampleShard(String id) {
		this.id = id;
	}
	
	@Override
	public void start() {
		consumer = vertx.eventBus().consumer("example."+id, msg -> {
			msg.reply("Echo from Shard "+id);
		});
	}
	
	@Override
	public void stop() {
		if (consumer != null)
			consumer.unregister();
	}
	
}
