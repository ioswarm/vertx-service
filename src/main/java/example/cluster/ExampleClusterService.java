package example.cluster;

import ioswarm.vertx.service.ClusteredService;

public class ExampleClusterService extends ClusteredService {

	@Override
	public String address() { return "example.cluster.service"; }

}
