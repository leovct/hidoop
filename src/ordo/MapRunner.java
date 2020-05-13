package ordo;

import java.rmi.Naming;
import formats.Format;
import map.Mapper;
import hdfs.NameNode;
import config.SettingsManager;
import java.io.File;


public class MapRunner extends Thread {

	Mapper m;
	Format reader, writer; 
	long jobId;
	String serverAddress;
	private static String messageHeader = ">>> [MAPRUNNER] ";


	public MapRunner(Mapper m, Format reader, Format writer, long jobId, String serverAddress){
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.jobId = jobId;
		this.serverAddress = serverAddress;
	}

	public void run() {
		// Open the file to read/write, execute the map task and close files
		if (reader != null) {
			System.out.println(messageHeader + "Starting map on file " + reader.getFname() + " ...");
			reader.open(Format.OpenMode.R);
		} else {
			System.out.println(messageHeader + "Starting map ...");
		}
		
		writer.open(Format.OpenMode.W);
		m.map(reader, writer);
		if (this.reader != null) {
			reader.close();
		}
		writer.close();

		// Preparing the parameter to send to NameNode/JobManager
		String chunkName = writer.getFname();
		long chunkSize = new File(chunkName).length();
		String[] chunkNameSplit = chunkName.split("/");
		String chunkNameWOPath = chunkNameSplit[chunkNameSplit.length-1];
		String filename = "";
		if (chunkNameWOPath.contains(SettingsManager.TAG_DATANODE)) {
			filename = (chunkNameWOPath.split(SettingsManager.TAG_DATANODE)[0]).split(SettingsManager.TAG_MAP)[1];
		} else {
			filename = chunkNameWOPath.split(SettingsManager.TAG_MAP)[1];
		}
		
		int chunkNumber = Integer.parseInt(chunkNameWOPath.split(SettingsManager.TAG_DATANODE)[1]);
		

		//Notify NameNode
		try {	
			NameNode nameNode = (NameNode) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.getPortNameNode()+"/NameNode");
			nameNode.chunkWritten(SettingsManager.TAG_MAP + filename, -1, (int)chunkSize, 1, chunkNumber, serverAddress);		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Notify JobManager
		try {	
			JobManager jobManager = (JobManager) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.getPortJobMaster()+"/JobManager");
			jobManager.notifyMapDone(jobId, chunkNumber, serverAddress);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (this.reader != null) {
			System.out.println(messageHeader + "Map on file " + reader.getFname() + " done !");
		} else {
			System.out.println(messageHeader + "Map done !");
		}
	}
}
