package ioswarm.vertx.log;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public interface Loggable {

	default Logger logger() { return LoggerFactory.getLogger(this.getClass()); }
	
}
