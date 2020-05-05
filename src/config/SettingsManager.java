package config;

/**
 * Java parameters for the project.
 */
public interface Project {

	/**
	 * Storage folder on servers.
	 * Must be the same as argument given to runner bash scripts.
	 */
	public String DATA_FOLDER = "/work/hidoop-fgvb/";

	/**
	 * Ports.
	 */
	public static int PORT_DAEMON = 4321;

	public static int PORT_NAMENODE = 4023;

	public static int PORT_DATANODE = 4027;

	public static int PORT_HDFSSERVEUR = 4698;

	/**
	 * NameNode server Address.
	 * Doesn't need to be configured here for HDFS application.
	 * Servers' addresses have to be written in file hidoop/config/servers,
	 * NameNode server first.
	 */
	public static String NAMENODE = "alose.enseeiht.fr";

	/**
	 * DataNodes servers Addresses.
	 * Doesn't need to be configured here for HDFS application.
	 * Servers' addresses have to be written in file hidoop/config/servers,
	 * NameNode server first.
	 */
	public static String DATANODES[] = {
			"bonite.enseeiht.fr", 
			"corb.enseeiht.fr", 
			"daurade.enseeiht.fr", 
			"eperlan.enseeiht.fr"
	};


	public final int megaOctet = 1000000;

	public final int mebiOctet = 1024*1024;

	/**
	 * Size of chunks.
	 */
	public static int CHUNK_SIZE = 64*mebiOctet;

	public enum Command{CMD_READ, CMD_WRITE, CMD_DELETE}
}
