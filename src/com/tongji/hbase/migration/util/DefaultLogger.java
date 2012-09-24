package com.tongji.hbase.migration.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.tongji.hbase.migration.common.Config;
import com.tongji.hbase.migration.common.ConfigDefs;

/**
 * The default logger.
 * 
 * @author Zhao Long
 */
public class DefaultLogger implements Logger, ConfigDefs {

	private static final SimpleDateFormat SDF = new SimpleDateFormat(
			"yy/MM/dd HH:mm:ss");
	private static final String LOG_FILE = Config.getProperties().getProperty(
			PROPERTY_GLOBAL_NAME)
			+ ".log";
	private static PrintStream out;

	static {
		try {
			File f = ResourceManager.getJarResourceAsFile(LOG_FILE,
					DefaultLogger.class);
			out = new PrintStream(new FileOutputStream(f, true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new IllegalArgumentException("Failed to Create Log File", e);
		}
	}

	private String className;

	/**
	 * Constructor.
	 */
	public DefaultLogger() {

	}

	@Override
	public void init(Class<?> clazz) {
		className = clazz.getSimpleName();
	}

	@Override
	public void setLevel(int aLevel) {

	}

	@Override
	public void info(String aMessage) {
		printOut("INFO", aMessage);
	}

	@Override
	public void warn(String aMessage) {
		printOut("WARN", aMessage);
	}

	@Override
	public void warn(String aMessage, Throwable aThrowable) {
		warn(aMessage + " exception=" + aThrowable);
		aThrowable.printStackTrace(out);
	}

	@Override
	public void error(String aMessage) {
		printErr("ERROR", aMessage);
	}

	@Override
	public void error(String aMessage, Throwable aThrowable) {
		error(aMessage + " exception=" + aThrowable);
		aThrowable.printStackTrace(out);
	}

	@Override
	public void trace(String aMessage) {
		warn(aMessage);
	}

	@Override
	public void debug(String aMessage) {
		warn(aMessage);
	}

	@Override
	public void fatal(String aMessage) {
		error(aMessage);
	}

	@Override
	public void fatal(String aMessage, Throwable aThrowable) {
		error(aMessage, aThrowable);
	}

	// Print message in System.out.
	private void printOut(String aTag, String aMessage) {
		out.println(getPrintString(aTag, aMessage));
	}

	// Print message in System.out.
	private void printErr(String aTag, String aMessage) {
		out.println(getPrintString(aTag, aMessage));
	}

	// Get the string to print.
	private String getPrintString(String aTag, String aMessage) {
		return SDF.format(new Date()) + " [" + aTag + "][" + className + "] "
				+ aMessage;
	}
}
