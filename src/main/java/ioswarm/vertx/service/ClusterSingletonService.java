package ioswarm.vertx.service;

import java.util.Random;

import io.vertx.core.json.JsonObject;

public abstract class ClusterSingletonService extends ClusteredService {

	private int sequence = 0;
	
	@Override
	public void start() {
		Random rand = new Random();
		sequence = rand.nextInt();
		super.start();
	}
	
	@Override
	protected JsonObject stateToken() {
		return super.stateToken().put("master.sequence", sequence);
	}
	
	protected boolean isMaster() {
		for (JsonObject o : neighbor.values())
			if (sequence < o.getInteger("master.sequence").intValue())
				return false;
		return true;
	}
	
}
