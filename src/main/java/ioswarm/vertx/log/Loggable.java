package ioswarm.vertx.log;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public interface Loggable {

	default Logger logger() { return LoggerFactory.getLogger(this.getClass()); }
	
	default void info(Object message) {
		if (logger().isInfoEnabled()) logger().info(message);
	}
	
	default void info(Object message, Throwable t) {
		if (logger().isInfoEnabled()) logger().info(message, t);
	}
	
	default void debug(Object message) {
		if (logger().isDebugEnabled()) logger().debug(message);
	}
	
	default void debug(Object message, Throwable t) {
		if (logger().isDebugEnabled()) logger().debug(message, t);
	}
	
	default void warn(Object message) {
		logger().warn(message);
	}
	
	default void warn(Object message, Throwable t) {
		logger().warn(message, t);
	}
	
	default void error(Object message) {
		logger().error(message);
	}
	
	default void error(Object message, Throwable t) {
		logger().error(message, t);
	}
	
}
