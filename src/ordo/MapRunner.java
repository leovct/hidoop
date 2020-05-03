package ordo;

import java.rmi.Naming;
import formats.Format;
import map.Mapper;
import hdfs.NameNode;
import config.Project;
import java.io.File;


public class MapRunner extends Thread {

	Mapper m; //map à lancer
	Format reader, writer; //les formats de lecture et d'écriture
	long jobId;
	String serverAddress;
	private static String messageHeader = ">>> [DAEMON] ";
	//private static String errorHeader = ">>> [ERROR] ";

	public MapRunner(Mapper m, Format reader, Format writer, long jobId, String serverAddress){
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.jobId = jobId;
		this.serverAddress = serverAddress;
	}

	public void run() {
		if (reader != null) {
			System.out.println(messageHeader + "Lancement du map sur le fichier " + reader.getFname());
			// Ouverture du fichier contenant le fragment sur lequel exécuter le map
			reader.open(Format.OpenMode.R);
		} else {
			System.out.println(messageHeader + "Lancement du map");
		}
		
		//Ouverture du fichier dans lequel les résultats du map doivent être écrits
		writer.open(Format.OpenMode.W);
		//Lancement du map sur le fragment
		m.map(reader, writer);
		//Fermeture des fichiers en lecture et écriture
		if (this.reader != null) {
			reader.close();
		}
		writer.close();

		// Préparation des paramètres
		String chunkName = writer.getFname();
		long chunkSize = new File(chunkName).length();
		String[] chunkNameSplit = chunkName.split("/");
		String chunkNameWOPath = chunkNameSplit[chunkNameSplit.length-1];
		String filename = chunkNameWOPath.split("-")[0];
		int chunkNumber;
		if (this.reader != null) {
			chunkNumber = Integer.parseInt(((chunkNameWOPath.split("-")[1]).split("\\.")[0]).split("(?<=\\D)(?=\\d)")[1]);
		} else {
			chunkNumber = Integer.parseInt(chunkNameWOPath.split("serverchunk")[1]);
		}
		

		//Notification au NameNode
		try {	
			NameNode nameNode = (NameNode) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode");
			if (this.reader != null) {
				nameNode.chunkWritten(filename+".txt-map", -1, (int)chunkSize, 1, chunkNumber, serverAddress);
			} else {
				nameNode.chunkWritten(chunkNameWOPath.split("-serverchunk")[0], -1, (int)chunkSize, 1, chunkNumber, serverAddress);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Notification du JobManager
		try {	
			JobManager jobManager = (JobManager) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/JobManager");
			jobManager.notifyMapDone(jobId, chunkNumber);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (this.reader != null) {
			System.out.println(messageHeader + "Map sur le fichier " + reader.getFname() + " terminé !");
		} else {
			System.out.println(messageHeader + "Map terminé !");
		}
	}
}
