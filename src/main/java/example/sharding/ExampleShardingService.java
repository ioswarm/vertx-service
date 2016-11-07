package example.sharding;

import io.vertx.core.Verticle;
import ioswarm.vertx.service.ShardingService;

public class ExampleShardingService extends ShardingService<String> {

	@Override
	protected Verticle createShardInstance(String key) {
		return new ExampleShard(key);
	}

	@Override
	public String address() { return "example"; }

}
