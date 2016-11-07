package example.cluster;

import com.hazelcast.config.Config;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
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
				vertx.deployVerticle(new ExampleClusterService());
			} else {
				throw new RuntimeException("Could not start cluster.", res.cause());
			}
		});
	}
	
}
