package config;

public interface Project {

	public static int NBR_DATANODE = 4;
	
	public static int PORT[] = {4001, 4002, 4003};
	
	public static int PORT_DAEMON = 4321;
	
	public static int PORT_NAMENODE = 4023;

	public static int PORT_HDFSSERVEUR = 4698;

	public static String NAMENODEHOST = "silure";
	
	public static String HOST[] = {"truite", "carpe", "omble", "tanche"}; // a modif
	
	public enum Commande {CMD_READ, CMD_WRITE, CMD_DELETE};
	
	public static int CHUNKSIZE = 1000000; // 100Mo en octet
}
