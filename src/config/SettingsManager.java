package config;

import java.io.File;

import formats.Format;
import formats.LineFormat;

public class SettingsManager {
	/**
	 * Command type for communication between HDFS entities.
	 */
	public enum Command{CMD_READ, CMD_WRITE, CMD_DELETE}

	/**
	 * Storage folder on servers.
	 * Must be the same as argument given to runner bash scripts.
	 */
	public static final String DATA_FOLDER = "/work/hidoop-fgvb/";

	/**
	 * Ports.
	 */
	public static final int PORT_DAEMON = 4321;
	public static final int PORT_NAMENODE = 4023;
	public static final int PORT_DATANODE = 4027;
	public static final int PORT_HDFSSERVEUR = 4698;
	
	/**
	 * Size of chunks.
	 */
	public static final int mebiOctet = 1024*1024;
	public static final int CHUNK_SIZE = 64*mebiOctet;
	
	/**
	 * 
	 */
	public static final String TAG_DATANODE = "-serverchunk";

	/**
	 * Path for servers configuration file.
	 */
	public static final String SERVERS_CONFIG = System.getProperty("java.class.path") + "/config/servers.config";
	
	/**
	 * Reads MasterNode's address from servers configuration file.
	 * 
	 * @return address of MasterNode's server.
	 */
	public static String getMasterNodeAddress() {
		if ((new File(SERVERS_CONFIG)).exists()) {
			LineFormat lineFormat = new LineFormat(SERVERS_CONFIG);
			lineFormat.open(Format.OpenMode.R);
			String value = lineFormat.read().v;
			lineFormat.close();
			return value;
		} else {
			System.err.println(">>> [SETTINGS] Could not load settings file "
					+ SERVERS_CONFIG);
			return null;
		}
	}
}
