package config;

import java.net.InetAddress;

public interface Project {
	
	public static int PORT[] = {4001, 4002, 4003};
	
	public static int PORT_DAEMON = 4321;
	
	public static int PORT_NAMENODE = 4023;
	
	public static int PORT_DATANODE = 4027;

	public static int PORT_HDFSSERVEUR = 4698;

	/**
	 * NameNode Adress
	 */
	public static String NAMENODE = "172.22.233.89";
	
	/**
	 * DataNodes Adresses
	 */
	/*public static String DATANODES[] = {
			"truite.enseeiht.fr", 
			"carpe.enseeiht.fr", 
			"omble.enseeiht.fr", 
			"tanche.enseeiht.fr"
			};
	*/
	public static String DATANODES[] = {"172.22.233.89"};
	
	public static int NUMBER_OF_DATANODE = DATANODES.length;
	
	public final int megaOctet = 1000000;
	
	public final int mebiOctet = 1024*1024;
	
	//public static int CHUNK_SIZE = 1000*megaOctet;
	public static int CHUNK_SIZE = 1*mebiOctet;
	
	public enum Command{CMD_READ, CMD_WRITE, CMD_DELETE}
	
	public String DATANODE_FILES_PATH = "data/filesample/";//"./work/fgvb";
	
	/**
	 * NOTE
	 * PC ILA : InetAddress.getLocalHost().getHostName() -> DESKTOP-G1FRCJ7
	 * PC N7  : InetAddress.getLocalHost().getHostName() -> name.enseeiht.fr
	 */
}
