package net.osmand;



import org.apache.commons.logging.Log;

/**
 * That class is replacing of standard LogFactory due to 
 * problems with Android implementation of LogFactory.
 * See Android analog of  LogUtil
 *
 * That class should be very simple & always use LogFactory methods,
 * there is an intention to delegate all static methods to LogFactory.
 */
public class PlatformUtil {
	
	public static Log getLog(Class<?> cl){
		return new Log() {
			
			@Override
			public void warn(Object arg0, Throwable arg1) {
				System.out.println(arg0);
			}
			
			@Override
			public void warn(Object arg0) {
				System.out.println(arg0);
			}
			
			@Override
			public void trace(Object arg0, Throwable arg1) {
				
			}
			
			@Override
			public void trace(Object arg0) {
				
			}
			
			@Override
			public boolean isWarnEnabled() {
				return true;
			}
			
			@Override
			public boolean isTraceEnabled() {
				return false;
			}
			
			@Override
			public boolean isInfoEnabled() {
				return true;
			}
			
			@Override
			public boolean isFatalEnabled() {
				return true;
			}
			
			@Override
			public boolean isErrorEnabled() {
				return true;
			}
			
			@Override
			public boolean isDebugEnabled() {
				return true;
			}
			
			@Override
			public void info(Object arg0, Throwable arg1) {
				System.out.println(arg0);				
			}
			
			@Override
			public void info(Object arg0) {
				System.out.println(arg0);				
			}
			
			@Override
			public void fatal(Object arg0, Throwable arg1) {
				System.out.println(arg0);				
			}
			
			@Override
			public void fatal(Object arg0) {
				System.out.println(arg0);				
			}
			
			@Override
			public void error(Object arg0, Throwable arg1) {
				System.out.println(arg0);				
			}
			
			@Override
			public void error(Object arg0) {
				System.out.println(arg0);				
			}
			
			@Override
			public void debug(Object arg0, Throwable arg1) {
				System.out.println(arg0);				
			}
			
			@Override
			public void debug(Object arg0) {
				System.out.println(arg0);
				
			}
		};
	}
	
}
