package hdfs;

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
import java.util.concurrent.ConcurrentHashMap;

import config.SettingsManager;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode {
	/**
	 * Constants.
	 */
	private static String messageHeader = ">>> [NAMENODE] ";
	private static String errorHeader = ">>> [ERROR] ";
	private static String metadataPrinting = ">>> [METADATA] ";
	private static final String noDataNodeError = errorHeader
			+ "No DataNode server avalaible";
	private static final String illegalReplicationFactorError = errorHeader
			+ "Replication factor must be strictly positive";
	private static final long serialVersionUID = 1L;
	private static final String backupFile = SettingsManager.DATA_FOLDER + "namenode-data";

	/**
	 * Metadata for files on the file system.
	 * #id : file name
	 * #value : FileData object storing file data and chunk handles 
	 */
	private ConcurrentHashMap<String, FileData> metadata;

	/**
	 * Reachable DataNodes (DataNodes known alive).
	 * Servers addresses.
	 */
	private ArrayList<String> avalaibleDataNodes;

	/**
	 * Replication factor of the NameNode.
	 */
	private int defaultReplicationFactor;

	/**
	 * Data writer of the NameNode.
	 */
	private DataWriter dataWriter;

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
				outputStream.close(); //FIXME save available DataNodes
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Default constructor.
	 * 
	 * @throws RemoteException
	 */
	public NameNodeImpl() throws RemoteException {
		if (!this.recoverData()) this.metadata = new ConcurrentHashMap<String, FileData>();
		this.avalaibleDataNodes = new ArrayList<String>();
		this.defaultReplicationFactor = 1;
		this.dataWriter = new DataWriter();
		this.printMetadata();
	}

	/**
	 * Constructor with specified default replication factor.
	 * 
	 * @param defaultReplicationFactor
	 * @throws RemoteException
	 */
	public NameNodeImpl(int defaultReplicationFactor) throws RemoteException {
		this();
		this.defaultReplicationFactor = defaultReplicationFactor;
	}

	@Override
	public ArrayList<String> writeChunkRequest() throws RemoteException {
		return this.writeChunkRequest(this.defaultReplicationFactor);
	}

	@Override
	public ArrayList<String> writeChunkRequest(int replicationFactor) throws RemoteException {
		// FIXME Implement best choice pick among available servers
		if (replicationFactor < 1) {
			System.err.println(illegalReplicationFactorError);
			return null;
		}
		int numberReturned = replicationFactor < this.avalaibleDataNodes.size() ?
				replicationFactor : this.avalaibleDataNodes.size();
		if (numberReturned > 0) {
			ArrayList<String> result = new ArrayList<String>();
			result.add(this.avalaibleDataNodes.remove(0)); //Adds first address of available server list
			this.avalaibleDataNodes.add(result.get(0)); //Moves to the end of available servers list
			for (int i = 0 ; i < numberReturned - 1 ; i++) {
				result.add(this.avalaibleDataNodes.get(i));
			}
			return result;
		} else {
			System.err.println(noDataNodeError);
			return null;
		}
	}

	@Override
	public ArrayList<ArrayList<String>> readFileRequest(String fileName) throws RemoteException {
		fileName = ignorePath(fileName);
		if (!this.metadata.containsKey(fileName)) {
			System.err.println(errorHeader + "File " + fileName
					+ " unknown to NameNode");
			return null;
		}
		if (!this.metadata.get(fileName).fileComplete()) {
			System.err.println(errorHeader + "Missing chunks"
					+ " information concerning file " + fileName);
			return null;
		}
		ArrayList<String> chunkHandles;
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		boolean avalaibleReplicaServer = false;
		FileData fileData = this.metadata.get(fileName);
		for(int chunk = 0 ; chunk < fileData.getFileSize() ; chunk++) {
			if (fileData.containsChunkHandle(chunk)) {
				chunkHandles = new ArrayList<String>();
				for (String server : fileData.getChunkHandle(chunk)) {
					if (this.avalaibleDataNodes.contains(server)) {
						chunkHandles.add(server);
						avalaibleReplicaServer = true;
					}
				}
				if (!avalaibleReplicaServer) {
					System.err.println(errorHeader + "No server containing a replica "
							+ "is avalaible for chunk " + chunk + " from file " + fileName);
					return null;
				} else {
					result.add(chunkHandles);
					avalaibleReplicaServer = false;
				}
			} else {
				System.err.println(errorHeader + "Chunk handle for chunk number "
						+ chunk + " could not be found for file " + fileName);
				return null;
			}
		}
		return result;
	}

	@Override
	public ArrayList<String> deleteFileRequest(String fileName) throws RemoteException {
		fileName = ignorePath(fileName);
		if (!this.metadata.containsKey(fileName)) {
			System.err.println(errorHeader + "File " + fileName
					+ " unknown to NameNode");
			return null;
		}
		ArrayList<String> result = new ArrayList<String>();
		FileData fileData = this.metadata.get(fileName);
		for(int chunk = 0 ; chunk < fileData.getFileSize() ; chunk++) {
			if (fileData.containsChunkHandle(chunk)) {
				for (String server : fileData.getChunkHandle(chunk)) {
					if (!this.avalaibleDataNodes.contains(server)) {
						if (!result.contains(server)) {
							System.err.println(errorHeader + "Server " + server 
									+ " is not available, could not delete chunk(s)"
									+ " from file " + fileName);
						}
					} else if (!result.contains(server)) result.add(server);
				}
			} else {
				System.err.println(errorHeader + "Chunk handle for chunk number "
						+ chunk + " could not be found for file " + fileName);
			}
		}
		return result;
	}

	@Override
	public void chunkWritten(String fileName, int fileSize, int chunkSize, int replicationFactor, int chunkNumber, String server) 
			throws RemoteException {
		FileData fileData;
		fileName = ignorePath(fileName);
		if (!this.metadata.containsKey(fileName)) {
			fileData = new FileData(fileSize, chunkSize, replicationFactor); //FIXME file size
			fileData.addChunkLocation(chunkNumber, server);
			this.metadata.put(fileName, fileData);
		} else {
			fileData = this.metadata.get(fileName);
			if (fileData.getChunkSize() != chunkSize
					&& fileData.getFileSize() == fileSize && fileData.getReplicationFactor() == replicationFactor) {
				//Received a chunk from a known file with an unknown chunk size : the chunk is the result of a map operation 
				fileData.setChunkSize(chunkSize); 
			} else if (fileData.getChunkSize() != chunkSize || fileData.getFileSize() != fileSize 
					|| fileData.getReplicationFactor() != replicationFactor) {
				fileData.setFileSize(fileSize);
				fileData.setChunkSize(chunkSize);
				fileData.setReplicationFactor(replicationFactor);
				fileData.getChunkHandles().clear();
				System.err.println(errorHeader + "Data for file "
						+ fileName + " has been overwritten by new ones");
			}
			fileData.addChunkLocation(chunkNumber, server);
		}
		(new Thread(this.dataWriter)).start(); //Run data writing in backup file
	}

	@Override
	public void allChunkWritten(String fileName) {
		FileData fileData;
		fileName = ignorePath(fileName);
		if (!this.metadata.containsKey(fileName)) { //Empty file
			fileData = new FileData(0, 0, 1);
			this.metadata.put(fileName, fileData);
		} else {
			fileData = metadata.get(fileName);
			fileData.setFileSize(fileData.getChunkHandles().size());
		}
		(new Thread(this.dataWriter)).start(); //Run data writing in backup file
		this.printMetadata();
	}

	@Override
	public void chunkDeleted(String fileName, int chunkNumber, String server) throws RemoteException {
		if (this.metadata.containsKey(fileName)) {
			FileData fileData = this.metadata.get(fileName);
			if (fileData.containsChunkHandle(chunkNumber)) {
				fileData.getChunkHandle(chunkNumber).remove(server);
				if (fileData.getChunkHandle(chunkNumber).isEmpty()) 
					fileData.getChunkHandles().remove(chunkNumber);
			}
			if (fileData.getChunkHandles().isEmpty()) {
				this.metadata.remove(fileName);
				this.printMetadata();
			}
		}
		(new Thread(this.dataWriter)).start(); //Run data writing in backup file
	}

	@Override
	public synchronized void notifyNameNodeAvailability(String serverAddress) throws RemoteException {
		if (!this.avalaibleDataNodes.contains(serverAddress)) this.avalaibleDataNodes.add(0, serverAddress);
		System.out.println(messageHeader + "DataNode running on " + serverAddress + " connected");
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
					this.metadata = (ConcurrentHashMap<String, FileData>) readObject;
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
	 * Returns name of the file located at given path.
	 * Ignores folder path.
	 * 
	 * @param filePath
	 * @return name of the file
	 */
	public String ignorePath(String filePath) {
		return ((filePath.contains("/")) ? filePath.substring(filePath.lastIndexOf('/')+1) : 
			((filePath.contains("\\")) ? filePath.substring(filePath.lastIndexOf('\\')+1) : filePath));
	}

	/**
	 * Prints metadata on output stream.
	 */
	private void printMetadata() {
		System.out.println(messageHeader + "PRINTING METADATA : ");
		if (this.metadata.isEmpty()) System.out.println(metadataPrinting + "No metadata");
		for (String file : this.metadata.keySet()) {
			System.out.println(metadataPrinting + "$" + file);
			System.out.println(metadataPrinting + "file size : "+this.metadata.get(file).getFileSize()
					+ ", chunk size : " + this.metadata.get(file).getChunkSize()
					+ ", replication factor : " + this.metadata.get(file).getReplicationFactor());
			for (int chunkHandle : this.metadata.get(file).getChunkHandles().keySet()) {
				System.out.println(metadataPrinting + "chunk "+chunkHandle+" : "+this.metadata.get(file).getChunkHandle(chunkHandle));
			}
		}
		System.out.println(metadataPrinting + "available DataNodes : " + this.avalaibleDataNodes);
	}

	/**
	 * Main.
	 * Initializes a NameNodeImpl instance and bounds it to the RMI registry.
	 * Optional argument [reset] can be use to delete any saved local metadata.
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(messageHeader + "NameNode starting...");
		if (args.length > 0 && args[0].equals("reset")) (new File(backupFile)).delete();
		try {//
			System.out.println(messageHeader + "Machine IP : " + InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		try{
			LocateRegistry.createRegistry(SettingsManager.PORT_NAMENODE);
		} catch(Exception e) {}
		try {
			Naming.bind("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode", new NameNodeImpl());
			System.out.println(messageHeader + "NameNode bound in registry");
			if (!(new File(SettingsManager.DATA_FOLDER).exists())) {
				(new File(SettingsManager.DATA_FOLDER)).mkdirs(); //Create data directory
			}
		} catch (AlreadyBoundException e) {
			System.err.println(errorHeader + "NameNode is already running on this server");
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}
}