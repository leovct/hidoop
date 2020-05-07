package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import config.SettingsManager;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.NameNode;
import map.MapReduce;

public class JobClient implements JobInterface {

    private Format.Type inputFormat;
	private Format.Type outputFormat;
	private Format.Type resReduceFormat;
	private String inputFName;
	private String outputFName;
	private String resReduceFName;
	private int nbMaps; 
	private List<ArrayList<String>> chunkList = new ArrayList<ArrayList<String>>();
	private long jobId;
	private static String messageHeader = ">>> [JOBCLIENT] ";


	// Constructor in case of map requiring file in input
	public JobClient(Format.Type inputFormat, String inputFName) {
		String temp = ((inputFName.contains("/")) ? inputFName.substring(inputFName.lastIndexOf('/')+1) : 
		((inputFName.contains("\\")) ? inputFName.substring(inputFName.lastIndexOf('\\')+1) : inputFName));
		this.inputFormat = inputFormat;
		this.inputFName = temp;
		this.outputFormat = Format.Type.KV;
		this.outputFName = SettingsManager.TAG_MAP + temp;
		this.resReduceFormat = Format.Type.KV;
		this.resReduceFName = SettingsManager.TAG_RESULT + temp;
	}
	
	// Constructor in case of map requiring no file in input
	public JobClient(String name) {
		this.inputFormat = null;
		this.inputFName = null;
		this.outputFormat = Format.Type.KV;
		this.outputFName = SettingsManager.TAG_MAP + name;
		this.resReduceFormat = Format.Type.KV;
		this.resReduceFName = SettingsManager.TAG_RESULT + name;
	}

	public void startJob (MapReduce mr) {

		System.out.println(messageHeader + "Submit job ...");
		
		// Create formats
		Format output = new KVFormat(getOutputFname());
		Format resReduce = new KVFormat(getResReduceFName());

		NameNode nm = null;
		JobManager jm = null;
		// Retrieving NameNode & JobManager
		try {
			System.out.println(messageHeader + "Retrieving stub of NameNode ...");
			nm = (NameNode)Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
			System.out.println(messageHeader + "Stub of NameNode retrieved !!");
			System.out.println(messageHeader + "Retrieving stub of JobManager");
			jm = (JobManager)Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/JobManager");
			System.out.println(messageHeader + "Stub of JobManager retrieved !!");
		} catch (Exception e) {
			e.printStackTrace();
		}

		//Initialize JobManager
		try {
			System.out.println(messageHeader + "Adding Job on JobManager...");
			long id = jm.addJob(mr, getInputFormat(), getInputFname());
			this.jobId = id;
			System.out.println(messageHeader + "Starting Job...");
			jm.startJob(id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Retrieving the list of daemons
		System.out.println(messageHeader + "Retrieving the list of Daemons ...");
		List<Daemon> demons = new ArrayList<Daemon>();
		List<String> demonsName = new ArrayList<String>();
		try {
			demonsName = jm.getAvalaibleDaemons();
		} catch (Exception e) {
			e.printStackTrace();
		}
		for(String serverAddress : demonsName) {
			try {
				System.out.println(messageHeader + "Retrieving stub of : //"+serverAddress+":"+SettingsManager.PORT_DAEMON+"/DaemonImpl" );
				demons.add((Daemon)Naming.lookup("//"+serverAddress+":"+SettingsManager.PORT_DAEMON+"/DaemonImpl"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(messageHeader + "Daemons retrieved !!");

		// In case of map requiring file in input
		if (this.inputFName != null) {
			//Retrieving the list of chunks
			try {
				System.out.println(messageHeader + "Retrieving the list of chunks ..."); 
				ArrayList<ArrayList<String>> chunks = nm.readFileRequest(getInputFname());
				setChunkList(chunks);
				System.out.println(messageHeader + "Chunks retrieved !!\n");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Update the number of maps according to the number of chunks
			setNbMaps(getChunkList().size());

		// In case of map requiring no file in input
		} else {
			// Perform one map per demon (ex. in the case of QuasiMonteCarlo)
			setNbMaps(demons.size());
		}
		
		// Starting maps on daemons
		System.out.println(messageHeader + "Starting maps ...");
		for(int i = 0; i < getNbMaps(); i++) {			
			String chunk;
			Daemon d;
			Format inputTmp, outputTmp;
			String machine = null;
			
			// In case of map requiring file in input
			if (this.inputFName != null) {
				// Retrieving the name of the chunk	
				chunk = getInputFname() + SettingsManager.TAG_DATANODE + i ;
				// Retrieving the names of the machines which possess the chunk
				ArrayList<String> machines = getChunkList().get(i); 
				// Retrieving the server chosen to execute the map
				try {
					machine = jm.submitMap(jobId, i, machines);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				int numDaemon = demonsName.indexOf(machine);
				d = demons.get(numDaemon);
				// Creating the formats for the chunk
				inputTmp = new LineFormat(SettingsManager.DATA_FOLDER + chunk);
				outputTmp = new KVFormat(SettingsManager.DATA_FOLDER + SettingsManager.TAG_MAP + chunk );

			// In case of map requiring no file in input
			} else {
				chunk = getOutputFname() + SettingsManager.TAG_DATANODE + i ;
				try {
					machine = jm.submitMap(jobId, i, null);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				int numDaemon = demonsName.indexOf(machine);
				d = demons.get(numDaemon);
				inputTmp = null;
				outputTmp = new KVFormat(SettingsManager.DATA_FOLDER + chunk);
			}
						
			// Call the method to run a map on a dameon
			try {
				d.runMap(mr, inputTmp, outputTmp, jobId);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println(messageHeader + "All maps started !!\n");

		// Waiting for all the maps to be done
		System.out.println(messageHeader + "Waiting for the callback of Daemons ...");
		try {
			int maps = jm.nbMapDone(jobId);
			int mapsTemp = 0;
			System.out.println(messageHeader + maps + "/" + this.nbMaps +" maps done");
			while(maps<nbMaps) {
				maps = jm.nbMapDone(jobId);
				if (maps>mapsTemp) {
					System.out.println(messageHeader + maps + "/" + this.nbMaps +" maps done");
					mapsTemp++;
				}
				
				
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println(messageHeader + "Callbacks received !!\n");

		// Notify NameNode that all chunk have been written
		try {
			nm.allChunkWritten(getOutputFname());
     		} catch (Exception e) {
			e.printStackTrace();
		}

		// Retrieving map results
		try {
			HdfsClient.HdfsRead(getOutputFname() , getOutputFname());
		} catch (Exception e) {
			e.printStackTrace();
		} 

		// Starting reduce
		output.open(Format.OpenMode.R);
		resReduce.open(Format.OpenMode.W);
		System.out.println(messageHeader + "Starting reduce ...");
		mr.reduce(output, resReduce);
		System.out.println(messageHeader + "Reduce done !!!");
		output.close();
		resReduce.close();
		System.out.println(messageHeader + "Job done !!");

	}


	public void setInputFormat(Format.Type format){
		this.inputFormat = format;
	}

	public Format.Type getInputFormat() {
		return this.inputFormat;
	}

	public void setInputFname(String fname){
		this.inputFName = fname;
	}

	public String getInputFname() {
		return this.inputFName;
	}

	public void setOutputFormat(Format.Type format){
		this.outputFormat = format;
	}

	public Format.Type getOutputFormat() {
		return this.outputFormat;
	}

	public void setOutputFname(String fname){
		this.outputFName = fname;
	}

	public String getOutputFname() {
		return this.outputFName;
	}

	public void setResReduceFormat(Format.Type format){
		this.resReduceFormat = format;
	}

	public Format.Type getResReduceFormat() {
		return this.resReduceFormat;
	}

	public void setResReduceFname(String fname){
		this.resReduceFName = fname;
	}

	public String getResReduceFName() {
		return this.resReduceFName;
	}


	public void setNbMaps(int maps){
		this.nbMaps = maps;
	}

	public int getNbMaps() {
		return this.nbMaps;
	}

	public void setChunkList(ArrayList<ArrayList<String>> chunks) {
		this.chunkList = chunks;
	}

	public List<ArrayList<String>> getChunkList() {
		return this.chunkList;
	}

}

