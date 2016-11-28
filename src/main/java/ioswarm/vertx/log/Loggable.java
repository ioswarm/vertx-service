package ioswarm.vertx.log;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public interface Loggable {

	default Logger log() { return LoggerFactory.getLogger(this.getClass()); }
	
	default void info(Object message) {
		if (log().isInfoEnabled()) log().info(message);
	}
	
	default void info(Object message, Throwable t) {
		if (log().isInfoEnabled()) log().info(message, t);
	}
	
	default void debug(Object message) {
		if (log().isDebugEnabled()) log().debug(message);
	}
	
	default void debug(Object message, Throwable t) {
		if (log().isDebugEnabled()) log().debug(message, t);
	}
	
	default void warn(Object message) {
		log().warn(message);
	}
	
	default void warn(Object message, Throwable t) {
		log().warn(message, t);
	}
	
	default void error(Object message) {
		log().error(message);
	}
	
	default void error(Object message, Throwable t) {
		log().error(message, t);
	}
	
}
