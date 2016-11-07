package example.sharding;

import com.hazelcast.config.Config;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class Example {

	public static void main(String[] args) throws Exception {
		Config config = new Config();
		ClusterManager mgr = new HazelcastClusterManager(config);
		VertxOptions opts = new VertxOptions().setClusterManager(mgr);
		Vertx.clusteredVertx(opts, res -> {
			if (res.succeeded()) {
				Vertx vertx = res.result();
				vertx.deployVerticle(new ExampleShardingService(), x -> {
					if (x.succeeded()) {
						final EventBus eb = vertx.eventBus();
						for (int i=1;i<=1001;i++) {
							final int id = i;
							DeliveryOptions opt = new DeliveryOptions().addHeader("shard", String.valueOf(id));
							eb.send("example", new JsonObject(), opt, hdl -> {
								if (hdl.succeeded()) 
									System.out.println("result: "+hdl.result().body()+" <- expect: "+id);
								else 
									System.out.println("error: "+hdl.cause().getMessage()+" <- expect: "+id);
							});
						}
					}
				});	
			} else {
				throw new RuntimeException("Could not start cluster.", res.cause());
			}
		});
	}
	
}
