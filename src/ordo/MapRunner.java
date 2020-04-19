package ordo;

import java.rmi.RemoteException;
import java.rmi.Naming;
import formats.Format;
import map.Mapper;
import hdfs.NameNode;
import config.Project;
import java.io.File;


public class MapRunner extends Thread {

	Mapper m; //map à lancer
	Format reader, writer; //les formats de lecture et d'écriture
	Callback cb;
	String serverAddress;
	private static String messageHeader = ">>> [DAEMON] ";
	private static String errorHeader = ">>> [ERROR] ";

	public MapRunner(Mapper m, Format reader, Format writer, Callback cb, String serverAddress){
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
		this.serverAddress = serverAddress;
	}

	public void run() {
		System.out.println(messageHeader + "Lancement du map sur le fichier " + reader.getFname());
		// Ouverture du fichier contenant le fragment sur lequel exécuter le map
		reader.open(Format.OpenMode.R);
		//Ouverture du fichier dans lequel les résultats du map doivent être écrits
		writer.open(Format.OpenMode.W);
		//Lancement du map sur le fragment
		m.map(reader, writer);
		//Fermeture des fichiers en lecture et écriture
		reader.close();
		writer.close();

		//Notification au NameNode
		try {	
			String chunkName = writer.getFname();
			long chunkSize = new File(chunkName).length();
			String[] chunkNameSplit = chunkName.split("/");
			String chunkNameWOPath = chunkNameSplit[chunkNameSplit.length-1];
			String filename = chunkNameWOPath.split("-")[0];
			int chunkNumber = Integer.parseInt(((chunkNameWOPath.split("-")[1]).split("\\.")[0]).split("(?<=\\D)(?=\\d)")[1]);
			NameNode nameNode = (NameNode) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode");
			nameNode.chunkWritten(filename+".txt-map", -1, (int)chunkSize, 1, chunkNumber, serverAddress);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Envoie du callback
		try {
			cb.incNbMapDone();   
		} catch (RemoteException e) {  
			e.printStackTrace();
		}

		System.out.println(messageHeader + "Map sur le fichier " + reader.getFname() + " terminé !");
	}
}
