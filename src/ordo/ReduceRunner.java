package ordo;

import java.rmi.Naming;
import formats.Format;
import map.Reducer;
import hdfs.NameNode;
import config.SettingsManager;
import java.io.File;


public class ReduceRunner extends Thread {

	Reducer r;
	Format reader, writer; 
	long jobId;
	String serverAddress;
	private static String messageHeader = ">>> [REDUCERUNNER] ";

	public ReduceRunner(Reducer r, Format reader, Format writer, long jobId, String serverAddress){
		this.r = r;
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
		r.reduce(reader, writer);
		if (this.reader != null) {
			reader.close();
		}
		writer.close();

		// Preparing the parameter to send to NameNode/JobManager
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
		

		//Notify NameNode
		try {	
			NameNode nameNode = (NameNode) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
			if (this.reader != null) {
				nameNode.chunkWritten(filename+".txt-map", -1, (int)chunkSize, 1, chunkNumber, serverAddress);
			} else {
				nameNode.chunkWritten(chunkNameWOPath.split(SettingsManager.TAG_DATANODE)[0], -1, (int)chunkSize, 1, chunkNumber, serverAddress);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Notify JobManager
		try {	
			JobManager jobManager = (JobManager) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/JobManager");
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