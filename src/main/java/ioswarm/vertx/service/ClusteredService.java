package ioswarm.vertx.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

public abstract class ClusteredService<T> extends Service {

	private final String id = UUID.randomUUID().toString();
	protected Map<String, JsonObject> neighbor = new HashMap<String, JsonObject>();
	private long aliveTimerId = 0l;
	protected MessageConsumer<T> msgConsumer;
	
	public String id() { return id; }
	
	public abstract String address();
	
	protected String address(String ... params) {
		String suffix = "";
		for (String p : params)
			suffix += "."+p;
		return address()+suffix;
	}
	
	protected String registerClusterServiceAddress(String ... params) { 
		return address(params)+".registerClusterService"; 
	}
	
	protected String isAliveClusterServiceAddress(String ... params) {
		return address(params)+".isAlive";
	}
	
	protected String refreshStateClusterServiceAddress(String ... params) {
		return address(params)+".refreshState";
	}
	
	protected String removeClusterServiceAddress(String ... params) {
		return address(params)+".removeClusterService";
	}
	
	protected JsonObject stateToken() {
		return new JsonObject().put("id", id);
	}
	
	@Override
	public void start() {
		final EventBus eb = vertx.eventBus();
		
		msgConsumer = eb.consumer(address(), this::onMessage);
		
		eb.consumer(registerClusterServiceAddress(), this::onRegisterClusterService);
		eb.consumer(registerClusterServiceAddress(id), this::onRegisterClusterService);
		
		eb.consumer(refreshStateClusterServiceAddress(), this::onRefreshStateClusterService);
		eb.consumer(refreshStateClusterServiceAddress(id), this::onRefreshStateClusterService);
		
		eb.consumer(removeClusterServiceAddress(), this::onRemoveClusterService);
		eb.consumer(removeClusterServiceAddress(id), this::onRemoveClusterService);
		
		eb.consumer(isAliveClusterServiceAddress(id), this::onIsAliveClusterService);
		aliveTimerId = vertx.setPeriodic(10000l, evt -> { // TODO config timer-intervall
			for (final String nid : neighbor.keySet())
				eb.send(isAliveClusterServiceAddress(nid), stateToken(), new DeliveryOptions().setSendTimeout(1000l), (AsyncResult<Message<JsonObject>> rpl) -> {
					if (!rpl.succeeded()) 
						vertx.eventBus().send(removeClusterServiceAddress(id), new JsonObject().put("id", nid));
					else
						onRefreshStateClusterService(rpl.result());
				});
		});
		
		try {
			onStart();
		} finally {
			eb.publish(registerClusterServiceAddress(), stateToken());
		}
	}
	
	@Override
	public void stop() {
		try {
			onStop();
		} finally {
			msgConsumer.unregister();
			
			vertx.cancelTimer(aliveTimerId);
		}
	}
	
	public void onStart() {
		
	}
	
	public void onStop() {
		
	}
	
	public void onMessage(Message<T> msg) {
		
	}
	
	public void onRegisterClusterService(Message<JsonObject> msg) {
		String fid = msg.body().getString("id");
		if (id.equals(fid) || neighbor.containsKey(fid)) return;
		info("register ClusterService "+address(fid)+" at "+address(id));

		neighbor.put(fid, msg.body());
		vertx.eventBus().send(registerClusterServiceAddress(fid), stateToken());
		
	}
	
	public void onRefreshStateClusterService(Message<JsonObject> msg) {
		String fid = msg.body().getString("id");
		if (id.equals(fid)) return;
		if (!neighbor.containsKey(fid)) onRegisterClusterService(msg);
		else neighbor.put(fid, msg.body());
	}
	
	public void onIsAliveClusterService(Message<JsonObject> msg) {
		if (!neighbor.containsKey(msg.body().getString("id"))) onRegisterClusterService(msg);
		msg.reply(stateToken());
	}
	
	public void onRemoveClusterService(Message<JsonObject> msg) {
		String fid = msg.body().getString("id");
		warn("ClusterService "+address(fid)+" is not alive ... remove from "+address(id));
		neighbor.remove(fid);
	}
}
