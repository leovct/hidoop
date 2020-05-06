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

public class JobClient {

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

	public String ignorePath(String filePath) {
		return ((filePath.contains("/")) ? filePath.substring(filePath.lastIndexOf('/')+1) : 
			((filePath.contains("\\")) ? filePath.substring(filePath.lastIndexOf('\\')+1) : filePath));
	}

	public JobClient(Format.Type inputFormat, String inputFName) {
		this.inputFormat = inputFormat;
		this.inputFName = ((inputFName.contains("/")) ? inputFName.substring(inputFName.lastIndexOf('/')+1) : 
			((inputFName.contains("\\")) ? inputFName.substring(inputFName.lastIndexOf('\\')+1) : inputFName));
		this.outputFormat = Format.Type.KV;
		this.outputFName = inputFName + "-map";
		this.resReduceFormat = Format.Type.KV;
		this.resReduceFName = inputFName + "-resf";
	}
	
	public JobClient(String name) {
		this.inputFormat = null;
		this.inputFName = null;
		this.outputFormat = Format.Type.KV;
		this.outputFName = name+"-map";
		this.resReduceFormat = Format.Type.KV;
		this.resReduceFName = name+"-resf";
	}

	public void startJob (MapReduce mr) {

		System.out.println(messageHeader + "Submit job ...");
		
		// Création des formats
		//Format input = new LineFormat(getInputFName());
		Format output = new KVFormat(getOutputFName());
		Format resReduce = new KVFormat(getResReduceFName());

		NameNode nm = null;
		JobManager jm = null;
		// Récupération du NameNode et JobManager
		try {
			System.out.println(messageHeader + "Récupération du stub du NameNode");
			nm = (NameNode)Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
			System.out.println(messageHeader + "Stub du NameNode récupéré !!");
			System.out.println(messageHeader + "Récupération du stub du JobManager");
			jm = (JobManager)Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/JobManager");
			System.out.println(messageHeader + "Stub du JobManager récupéré !!");
		} catch (Exception e) {
			e.printStackTrace();
		}

		//Initialisation côté JobManager
		try {
			System.out.println(messageHeader + "Ajout du Job au JobManager...");
			long id = jm.addJob(mr, getInputFormat(), getInputFName());
			this.jobId = id;
			System.out.println(messageHeader + "Lancement du Job...");
			jm.startJob(id);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Récupération de la liste des démons
		System.out.println(messageHeader + "Récupération de la liste des Daemons ...");
		List<Daemon> demons = new ArrayList<Daemon>();
		List<String> demonsName = new ArrayList<String>();
		try {
			demonsName = jm.getAvalaibleDaemons();
		} catch (Exception e) {
			e.printStackTrace();
		}
		for(String serverAddress : demonsName) {
			try {
				System.out.println(messageHeader + "On récupère le stub de : //"+serverAddress+":"+SettingsManager.PORT_DAEMON+"/DaemonImpl" );
				demons.add((Daemon)Naming.lookup("//"+serverAddress+":"+SettingsManager.PORT_DAEMON+"/DaemonImpl"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(messageHeader + "Daemons récupérés !!");

		if (this.inputFName != null) {
			//Récupération de la liste des chunks
			try {
				System.out.println(messageHeader + "Récupération de la liste des chunks ..."); 
				ArrayList<ArrayList<String>> chunks = nm.readFileRequest(getInputFName());
				setChunkList(chunks);
				System.out.println(messageHeader + "Chunks récupérés !!\n");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Mise à jour du nombre de maps à effectuer
			setNbMaps(getChunkList().size());
		} else {
			// Effectuer un map par démon (ex. dans le cas de QuasiMonteCarlo)
			setNbMaps(demons.size());
		}
		
		// Lancement des maps sur les démons
		System.out.println(messageHeader + "Lancement des maps ...");
		for(int i = 0; i < getNbMaps(); i++) {			
			String chunk;
			Daemon d;
			Format inputTmp, outputTmp;
			if (this.inputFName != null) {
				// On définit le nom du chunk
				chunk = getInputFName().split("\\.")[0] + SettingsManager.TAG_DATANODE + i + "." + getInputFName().split("\\.")[1];
				// On récupère le nom des machines qui possède le chunk
				ArrayList<String> machines = getChunkList().get(i); 
				//On récupère le serveur qui s'occupera du map
				String machine = null;
				try {
					machine = jm.submitMap(jobId, i, machines);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				// On récupère le numéro du démon sur lequel lancer le map
				int numDaemon = demonsName.indexOf(machine);
				// On récupère le bon démon dans la liste des démons
				d = demons.get(numDaemon);
				// On change le nom des Formats pour qu'ils correspondent aux fragments
				inputTmp = new LineFormat(SettingsManager.DATA_FOLDER + chunk);
				outputTmp = new KVFormat(SettingsManager.DATA_FOLDER + chunk + "-map");
			} else {
				chunk = getOutputFName() + SettingsManager.TAG_DATANODE + i;
				String machine = null;
				try {
					machine = jm.submitMap(jobId, i, null);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				// On récupère le numéro du démon sur lequel lancer le map
				int numDaemon = demonsName.indexOf(machine);
				// On récupère le bon démon dans la liste des démons
				d = demons.get(numDaemon);
				inputTmp = null;
				outputTmp = new KVFormat(SettingsManager.DATA_FOLDER + chunk);
			}
			
			// On change le nom des Formats pour qu'ils correspondent aux fragments
			
			// On appelle runMap sur le bon démon
			try {
				d.runMap(mr, inputTmp, outputTmp, jobId);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println(messageHeader + "Lancement des maps terminé !!\n");

		// Puis on attends que tous les démons aient finis leur travail
		System.out.println(messageHeader + "Attente du callback des Daemons ...");
		try {
			int maps = jm.nbMapDone(jobId);
			int mapsTemp = 0;
			System.out.println(messageHeader + maps + "/" + this.nbMaps +" maps effectués");
			while(maps<nbMaps) {
				maps = jm.nbMapDone(jobId);
				if (maps>mapsTemp) {
					System.out.println(messageHeader + maps + "/" + this.nbMaps +" maps effectués");
					mapsTemp++;
				}
				
				
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		System.out.println(messageHeader + "Callbacks reçus !!\n");

		// Notifier le NameNode que tous les chunks ont été écrits
		try {
			nm.allChunkWritten(getOutputFName());
     		} catch (Exception e) {
			e.printStackTrace();
		}

		// On appelle hdfs.read pour récupérer tous les résultats des maps
		try {
			HdfsClient.HdfsRead(getOutputFName() , getOutputFName());
		} catch (Exception e) {
			e.printStackTrace();
		} 

		// On peut alors lancer le reduce
		output.open(Format.OpenMode.R);
		resReduce.open(Format.OpenMode.W);
		System.out.println(messageHeader + "Lancement du reduce ...");
		mr.reduce(output, resReduce);
		System.out.println(messageHeader + "Reduce terminé !!!");
		output.close();
		resReduce.close();
		System.out.println(messageHeader + "Job terminé !!");

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

	public String getInputFName() {
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

	public String getOutputFName() {
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

