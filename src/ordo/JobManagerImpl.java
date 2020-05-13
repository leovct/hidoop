package ordo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import config.SettingsManager;
import formats.Format.Type;
import map.MapReduce;

public class JobManagerImpl extends UnicastRemoteObject implements JobManager {

    /**
	 * Constants.
	 */
	private static String messageHeader = ">>> [JOBMANAGER] ";
	private static String errorHeader = ">>> [ERROR] ";
	private static String metadataPrinting = ">>> [METADATA] ";
	//private static final String noDaemonError = errorHeader
	//		+ "No Daemon avalaible";
	private static final String backupFile = SettingsManager.getDataPath() + "jobmanager-data";

	/**
	 * Metadata for files on the file system.
	 * #id : identifier of the job
	 * #value : JobData object storing details about the job
	 */
	private ConcurrentHashMap<Long, JobData> metadata;

	/**
	 * Reachable Daemons (Daemons known alive).
	 * Server addresses.
	 */
	private ConcurrentHashMap<String, Integer> avalaibleDaemons;

	/**
	 * Data writer of the NameNode.
	 */
	//private DataWriter dataWriter;


	/**
	 * Runnable class saving NameNode data into a backup local file.
	 * (Nested class)
	 */
	class DataWriter implements Runnable {
		@Override
		public void run() {
			this.writeData();
		}
		private synchronized void writeData() {
			try {
				ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(backupFile));
				outputStream.writeObject(metadata);
				outputStream.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

    protected JobManagerImpl() throws RemoteException {
        if (!this.recoverData()) this.metadata = new ConcurrentHashMap<Long, JobData>();
		this.avalaibleDaemons = new ConcurrentHashMap<String, Integer>();
		//this.dataWriter = new DataWriter();
		this.printMetadata();
    }

    private static final long serialVersionUID = 1L;

    @Override
    public long addJob(MapReduce mapperReducer, Type formatType, String filename) throws RemoteException {
		JobData jobData = new JobData(filename, formatType, mapperReducer);
		long id = System.currentTimeMillis();
		while (this.metadata.containsKey(id)) {
			id = System.currentTimeMillis();
		}
		this.metadata.put(id, jobData);
		//(new Thread(this.dataWriter)).start(); //Run data writing in backup file
		this.printMetadata();
		return id;		
    }

    @Override
    public void startJob(long jobId) throws RemoteException {
		if (!this.metadata.containsKey(jobId)) {
			System.err.println(errorHeader + "Job " + jobId
					+ " unknown to JobManager");
		} else {
			JobData jobData = this.metadata.get(jobId);
			jobData.setJobState(State.Running);
			this.metadata.put(jobId, jobData);
			//(new Thread(this.dataWriter)).start(); //Run data writing in backup file
			this.printMetadata();
		}        
    }

    @Override
    public void deleteJob(long jobId) throws RemoteException {
        if (!this.metadata.containsKey(jobId)) {
			System.err.println(errorHeader + "Job " + jobId
					+ " unknown to JobManager");
		} else {
			this.metadata.remove(jobId);
			//(new Thread(this.dataWriter)).start(); //Run data writing in backup file
			this.printMetadata();
		} 
	}

	@Override
	public String submitMap(long jobId, int chunkId, ArrayList<String> servers) throws RemoteException {
		System.out.println("-------SubmitJob---------");
		if (!this.metadata.containsKey(jobId)) {
			System.err.println(errorHeader + "Job " + jobId
					+ " unknown to JobManager");
		} else {
			JobData jobData = this.metadata.get(jobId);
			jobData.addMapState(chunkId);
			this.metadata.put(jobId, jobData);
			//(new Thread(this.dataWriter)).start(); //Run data writing in backup file
			this.printMetadata();
		}

		// Retourne la liste des serveurs dans l'ordre croissant du nb de maps
		Set<Entry<String,Integer>> entries = this.avalaibleDaemons.entrySet();     
		List<Entry<String,Integer>> sortedEntries = new ArrayList<>(entries);
		Collections.sort(sortedEntries, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));

		String serverAddress;
		if (servers == null){
			// Si on a un map sans file en entrée, on retourne le serveur avec le moins de map running
			serverAddress = sortedEntries.get(0).getKey();
			this.avalaibleDaemons.merge(serverAddress, 1, (a,b) -> a+b);
			return serverAddress;
		} else {
			// Sinon, on retourne le serveur avec le moins de map running parmi la liste des serveurs possédant le chunk
			for (Entry<String,Integer> entry : sortedEntries){
				if (servers.contains(entry.getKey())) {
					serverAddress = entry.getKey();
					this.avalaibleDaemons.merge(serverAddress, 1, (a,b) -> a+b);
					return serverAddress;
				}
			}
		}
		return null;
	}
	
	@Override
	public void notifyMapDone(long jobId, int chunkId, String serverAddress) throws RemoteException {
		System.out.println("-------MapDone---------");
		if (!this.metadata.containsKey(jobId)) {
			System.err.println(errorHeader + "Job " + jobId
					+ " unknown to JobManager");
		} else {
			JobData jobData = this.metadata.get(jobId);
			jobData.setMapState(chunkId, true);
		}

		if (!this.avalaibleDaemons.containsKey(serverAddress)) {
			System.err.println(errorHeader + "Server " + serverAddress
					+ " unknown to JobManager");
		} else {
			this.avalaibleDaemons.merge(serverAddress, 1, (a,b) -> a-b);
		}

		//(new Thread(this.dataWriter)).start(); //Run data writing in backup file
		this.printMetadata();
	}

	@Override
	public int nbMapDone(long jobId) throws RemoteException{
			JobData jobData = this.metadata.get(jobId);
			return jobData.getNbMapsDone();
	}

    @Override
    public void notifyDaemonAvailability(String serverAddress) throws RemoteException {
        if (!this.avalaibleDaemons.containsKey(serverAddress)) {
			this.avalaibleDaemons.put(serverAddress,0);
		}
		System.out.println(messageHeader + "Daemon running on " + serverAddress + " connected");

    }

    @Override
    public ArrayList<String> getAvalaibleDaemons() throws RemoteException {
        if (this.avalaibleDaemons.isEmpty()) {
			return null;
		} else {
			ArrayList<String> daemons = new ArrayList<String>();
			for (String avDaemons : this.avalaibleDaemons.keySet()) {
				daemons.add(avDaemons);
			}
			return daemons;
		}
    }

    /**
	 * Load NameNode data from an existing backup local file.
	 * 
	 * @return boolean, true if data could be loaded
	 */
	@SuppressWarnings("unchecked")
	private boolean recoverData() {
		if ((new File(backupFile).exists())) {
			try {
				ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(backupFile));
				Object readObject = inputStream.readObject();
				if (readObject instanceof ConcurrentHashMap<?,?>) {
					this.metadata = (ConcurrentHashMap<Long, JobData>) readObject;
					inputStream.close();
					return true;
				} else {
					System.err.println(errorHeader + "Content of backup file "
							+ backupFile + " is corrupted, could not load data");
					inputStream.close();
					return false;
				}
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		} else return false;
	}

	/**
	 * Prints metadata on output stream.
	 */
	private void printMetadata() {
		System.out.println(messageHeader + "PRINTING METADATA : ");
		if (this.metadata.isEmpty()) System.out.println(metadataPrinting + "No metadata");
		for (long id : this.metadata.keySet()) {
			System.out.println(metadataPrinting + "$" + id);
			System.out.println(metadataPrinting + "file : "+this.metadata.get(id).getFileName()
					+ ", file type : " + this.metadata.get(id).getFileType()
                    + ", map/reduce application : " + this.metadata.get(id).getMapperReducer().getClass().getName() + ", Status : " + this.metadata.get(id).getJobState());
            if (this.metadata.get(id).getJobState() == State.Suspended || this.metadata.get(id).getJobState() == State.Running ) {
                System.out.println(metadataPrinting + "Number of maps done : "+this.metadata.get(id).getNbMapsDone() + "/" + this.metadata.get(id).getMapState().size());
            }
		}
		System.out.println(metadataPrinting + "available Daemons : " + this.avalaibleDaemons);
    }
    
    /**
	 * Main.
	 * Initializes a NameNodeImpl instance and bounds it to the RMI registry.
	 * Optional argument [reset] can be use to delete any saved local metadata.
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(messageHeader + "JobManager starting...");
		if (args.length > 0 && args[0].equals("reset")) (new File(backupFile)).delete();
		try {//
			System.out.println(messageHeader + "Machine IP : " + InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		try{
			LocateRegistry.createRegistry(SettingsManager.getPortJobMaster());
		} catch(Exception e) {}
		try {
			Naming.bind("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.getPortJobMaster()+"/JobManager", new JobManagerImpl());
			System.out.println(messageHeader + "JobManager bound in registry");
			if (!(new File(SettingsManager.getDataPath()).exists())) {
				(new File(SettingsManager.getDataPath())).mkdirs(); //Create data directory
			}
		} catch (AlreadyBoundException e) {
			System.err.println(errorHeader + "JobManager is already running on this server");
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}
}

