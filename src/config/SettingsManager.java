package config;

import java.io.File;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

public class SettingsManager {
	/**
	 * Message header.
	 */
	private static String messageHeader = ">>> [SETTINGS] ";
	
	/**
	 * Command type for communication between HDFS entities.
	 */
	public enum Command{CMD_READ, CMD_WRITE, CMD_DELETE}

	/**
	 * Tags in fileNames.
	 */
	public static final String TAG_DATANODE = "-serverchunk";
	public static final String TAG_MAP = "map-";
	public static final String TAG_REDUCE = "red-";
	public static final String TAG_RESULT = "resf-";
	
	/**
	 * Path to servers configuration file.
	 */
	public static final String SERVERS_CONFIG = System.getProperty("java.class.path") + "/config/servers.config";
	
	/**
	 * Path to settings configuration file.
	 */
	public static final String SETTINGS_CONFIG = System.getProperty("java.class.path") + "/config/settings.config";
	
	/**
	 * Keys in KV settings file.
	 */
	private final static String keyChunkSize = "chunksize";
	private final static String keyDataPath = "datapath";
	private final static String keyPortNameNode = "portnamenode";
	private final static String keyPortDataNode = "portdatanode";
	private final static String keyPortJobMaster = "portjobmaster";
	private final static String keyPortDaemon = "portdaemon";
	
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
			System.err.println(messageHeader + "Could not load servers file "
					+ SERVERS_CONFIG);
			return null;
		}
	}
	
	/**
	 * Reads setting value from settings file.
	 * 
	 * @param key of the setting in setting file
	 * @return setting written in settings file 
	 */
	private static String ReadSettingValue(String key) {
		if ((new File(SETTINGS_CONFIG)).exists()) {
			KVFormat KVFormat = new KVFormat(SETTINGS_CONFIG);
			KVFormat.open(Format.OpenMode.R);
			KV value;
			while(((value = KVFormat.read()) != null) && !value.k.equals(key));
			KVFormat.close();
			if (value == null) {
				System.err.println(messageHeader + key 
						+ " inexistant in settings file " + SETTINGS_CONFIG);
				return null;
			}
			return value.v;
		} else {
			System.err.println(messageHeader + "Could not load settings file "
					+ SETTINGS_CONFIG);
			return null;
		}
	}
	
	/**
	 * Reads chunk size from settings file.
	 * 
	 * @return size of chunks, in mebiOctet.
	 */
	public static int getChunkSize() {
		try {
			return 1024*1024 * Integer.parseInt(ReadSettingValue(keyChunkSize));
		} catch (NumberFormatException e) {
			System.err.println(messageHeader + "Chunk size setting (mo) "
					+ "must be an integer");
			return -1;
		}
	}
	
	/**
	 * Reads data path value from settings file.
	 * 
	 * @return path to data on servers.
	 */
	public static String getDataPath() {
		return ReadSettingValue(keyDataPath);
	}
	
	/**
	 * Reads NameNode's port from settings file.
	 * 
	 * @return NameNode's port.
	 */
	public static int getPortNameNode() {
		try {
			return Integer.parseInt(ReadSettingValue(keyPortNameNode));
		} catch (NumberFormatException e) {
			System.err.println(messageHeader + "NameNode's port setting "
					+ "must be an integer");
			return -1;
		}
	}
	
	/**
	 * Reads DataNode's port from settings file.
	 * 
	 * @return DataNode's port.
	 */
	public static int getPortDataNode() {
		try {
			return Integer.parseInt(ReadSettingValue(keyPortDataNode));
		} catch (NumberFormatException e) {
			System.err.println(messageHeader + "DataNode's port setting "
					+ "must be an integer");
			return -1;
		}
	}
	
	/**
	 * Reads JobMaster's port from settings file.
	 * 
	 * @return JobMaster's port.
	 */
	public static int getPortJobMaster() {
		try {
			return Integer.parseInt(ReadSettingValue(keyPortJobMaster));
		} catch (NumberFormatException e) {
			System.err.println(messageHeader + "JobMaster's port setting "
					+ "must be an integer");
			return -1;
		}
	}
	
	/**
	 * Reads Daemon's port from settings file.
	 * 
	 * @return Daemon's port.
	 */
	public static int getPortDaemon() {
		try {
			return Integer.parseInt(ReadSettingValue(keyPortDaemon));
		} catch (NumberFormatException e) {
			System.err.println(messageHeader + "Daemon's port setting "
					+ "must be an integer");
			return -1;
		}
	}
}
