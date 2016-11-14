package ioswarm.vertx.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.RemovalNotification;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.MultiMap;
import io.vertx.core.Verticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

public abstract class ShardingService<T> extends ClusteredService<T> {

	protected Map<String, ShardEntry> shards = new ConcurrentHashMap<String, ShardEntry>(); 
	protected long removeShardTimerId = 0l;
	
	protected abstract Verticle createShardInstance(String key);
	
	@Override
	public void start() {
		
		removeShardTimerId = vertx.setPeriodic(5000l, hdl -> { // TODO configure interval
			for (final ShardEntry entry : shards.values()) 
				if (entry.getTimestamp() < (System.currentTimeMillis()-(5*60*1000))) // TODO configure ttl
					removeShard(entry);
		});
		
		super.start();
	}
	
	@Override
	public void stop() {
		vertx.cancelTimer(removeShardTimerId);
		
		for (final ShardEntry entry : shards.values())
			removeShard(entry);
		
		super.stop();
	}
	
	protected void removeShard(final ShardEntry entry) {
		vertx.undeploy(entry.getDeploymentId(), hdl -> {
			if (hdl.succeeded()) {
				shards.remove(entry.getKey());
				info("Removed shard: "+address(entry.getKey())+" - deploymentId: "+entry.getDeploymentId());
				// TODO send to BUS ?!?
			} else
				warn("Could not remove shard: "+address(entry.getKey())+" - deploymentId: "+entry.getDeploymentId(), hdl.cause());
		});
	}
	
	public void removeShard(final RemovalNotification<String, String> evt) {
		vertx.undeploy(evt.getValue(), hdl -> {
			if (hdl.succeeded()) {
				info("Removed shard: "+address(evt.getKey())+" - deploymentId: "+evt.getValue());
				// TODO send to BUS ?!?
			} else
				warn("Could not remove shard: "+address(evt.getKey())+" - deploymentId: "+evt.getValue(), hdl.cause());
		});
	}
	
	protected DeliveryOptions createFromHeader(MultiMap map) {
		return new DeliveryOptions()
				.setHeaders(map)
				.setSendTimeout(5*60*1000); // TODO configure
	}
	
	@Override
	public void onMessage(final Message<T> msg) {
		if (msg.headers().get("shard") != null) {
			if (!shards.containsKey(msg.headers().get("shard"))) {
//				final Verticle v = createShardInstance(msg.headers().get("shard"));
//				DeploymentOptions ops = new DeploymentOptions().setWorker(true);
//				vertx.deployVerticle(v, ops, cpl -> {
//					if (cpl.succeeded()) {
//						shards.put(msg.headers().get("shard"), new ShardEntry(msg.headers().get("shard"), cpl.result()));
//						info("Shard "+address(msg.headers().get("shard"))+" deployed.");
//						
//						vertx.eventBus().send(address(msg.headers().get("shard")), msg.body(), createFromHeader(msg.headers()), rpl -> {
//							if (rpl.succeeded()) msg.reply(rpl.result().body());
//							else {
//								warn("Could not send message to shard(1) ... "+msg.headers().get("shard")+".", rpl.cause());
//								msg.fail(-2, rpl.cause().getMessage());
//							}
//						});
//					} else {
//						error("ERROR while deploy shard for "+msg.headers().get("shard")+".", cpl.cause());
//						msg.fail(-3, cpl.cause().getMessage());
//					}
//				});
				
				
				vertx.executeBlocking(f -> {
					final Verticle v = createShardInstance(msg.headers().get("shard"));
					DeploymentOptions ops = new DeploymentOptions().setWorker(true);
					vertx.deployVerticle(v, ops, cpl -> {
						if (cpl.succeeded()) {
							shards.put(msg.headers().get("shard"), new ShardEntry(msg.headers().get("shard"), cpl.result()));
							info("Shard "+address(msg.headers().get("shard"))+" deployed.");
							f.complete(cpl.result());
						} else {
							f.fail(cpl.cause());
						}
					});
				}, res -> {
					if (res.succeeded()) {
						info("send shard 1");
						vertx.eventBus().send(address(msg.headers().get("shard")), msg.body(), createFromHeader(msg.headers()), rpl -> {
							if (rpl.succeeded()) msg.reply(rpl.result().body());
							else {
								warn("Could not send message to shard(1) ... "+msg.headers().get("shard")+".", rpl.cause());
								msg.fail(-2, rpl.cause().getMessage());
							}
						});
					} else {
						error("ERROR while deploy shard for "+msg.headers().get("shard")+".", res.cause());
						msg.fail(-3, res.cause().getMessage());
					}
				});
			} else {
				shards.put(msg.headers().get("shard"), shards.get(msg.headers().get("shard")).refresh());
				info("send shard 2");
				vertx.eventBus().send(address(msg.headers().get("shard")), msg.body(), createFromHeader(msg.headers()), rpl -> {
					if (rpl.succeeded()) msg.reply(rpl.result().body());
					else {
						warn("Could not send message to shard(2) ... "+msg.headers().get("shard")+".", rpl.cause());
						msg.fail(-2, rpl.cause().getMessage());
					}
				});
			}
		} else super.onMessage(msg);
	}
	
	protected class ShardEntry {
		private String key;
		private String deploymentId;
		private long timestamp;
		
		public ShardEntry() {}
		
		public ShardEntry(String key, String deploymentId) {
			setKey(key);
			setDeploymentId(deploymentId);
			setTimestamp(System.currentTimeMillis());
		}
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getDeploymentId() {
			return deploymentId;
		}
		public void setDeploymentId(String deploymentId) {
			this.deploymentId = deploymentId;
		}
		public long getTimestamp() {
			return timestamp;
		}
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
		
		public ShardEntry refresh() {
			setTimestamp(System.currentTimeMillis());
			return this;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((deploymentId == null) ? 0 : deploymentId.hashCode());
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
			return result;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ShardEntry other = (ShardEntry) obj;
			if (deploymentId == null) {
				if (other.deploymentId != null)
					return false;
			} else if (!deploymentId.equals(other.deploymentId))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			if (timestamp != other.timestamp)
				return false;
			return true;
		}
		
	}
	
}
