package hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import config.Project;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode {
	private static final long serialVersionUID = 1L;
	private static final int timeout = 400;
	private static final String backupFile = "data/namenode-data";

	/**
	 * Méta-données (en mémoire) du NameNode :
	 * #id : nom du fichier
	 * #value : liste qui contient les informations suivantes
	 * 		- taille du fichier
	 * 		- taille des chunk
	 * 		- facteur de réplication du fichier
	 * 		- id des serveurs contenant les bloc (index dans le tableau Project.DATANODES)
	 */
	private ConcurrentHashMap<String, FileData> metadata;

	/**
	 * Reachable DataNodes (DataNodes known alive)
	 * each integer corresponds to the index of the server address in Project.DATANODES
	 */
	private ArrayList<String> avalaibleServers;

	/**
	 * Replication factor of the NameNode
	 */
	private int defaultReplicationFactor;

	/**
	 * Data writer of the NameNode
	 */
	private DataWriter dataWriter;


	/**
	 * Runnable class saving NameNode data into a backup local file
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

	/**
	 * Default constructor
	 * @throws RemoteException
	 */
	public NameNodeImpl() throws RemoteException {
		if (!this.recoverData()) this.metadata = new ConcurrentHashMap<String, FileData>();
		this.avalaibleServers = new ArrayList<String>();
		this.defaultReplicationFactor = 1;
		this.dataWriter = new DataWriter();
		for (String server : Project.DATANODES) {
			try {
				if (InetAddress.getByName(server).isReachable(timeout)) { //FIXME Changer le check : vérifier que l'objet DataNode répond bien
					this.avalaibleServers.add(server);
				}
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.printMetaData();
	}

	/**
	 * Constructor with specified default replication factor
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
		// FIXME Implémenter le meilleur choix parmi les serveurs dispos
		int numberReturned = this.defaultReplicationFactor > this.avalaibleServers.size() ?
				this.defaultReplicationFactor : this.avalaibleServers.size();
		if (numberReturned > 0) {
			ArrayList<String> result = new ArrayList<String>();
			Collections.shuffle(this.avalaibleServers); //Random server pick
			for (int i = 0 ; i < numberReturned ; i++) {
				result.add(this.avalaibleServers.get(i));
			}
			return result;
		} else {
			System.err.println("#writeChunkRequest : No DataNode server avalaible");
			return null;
		}
	}

	@Override
	public ArrayList<String> readFileRequest(String fileName) throws RemoteException {
		// FIXME FIXME add dans metadata : ici ou suite à une notification du datanode?
		if (!this.metadata.containsKey(fileName)) {
			System.err.println("#readFileRequest : file " + fileName
					+ " unknown to NameNode");
			return null;
		}
		if (!this.metadata.get(fileName).fileComplete()) {
			System.err.println("#readFileRequest : missing chunks"
					+ " information concerning file " + fileName);
			return null;
		}
		ArrayList<String> result = new ArrayList<String>();
		boolean avalaibleReplicaServer = false;
		FileData fileData = this.metadata.get(fileName);
		for(int chunk = 0 ; chunk < fileData.getFileSize()/fileData.getReplicationFactor() ; chunk++) {
			if (fileData.containsChunkHandle(chunk)) {
				for (String server : fileData.getChunkHundle(chunk)) {
					if (this.avalaibleServers.contains(server)) {
						result.add(server);
						avalaibleReplicaServer = true;
						break; //FIXME RENVOYER UN SEUL SERVEUR?
					}
					if (!avalaibleReplicaServer) {
						System.err.println("#readFileRequest : No server containing a replica "
								+ "is avalaible for chunk " + chunk + " from file " + fileName);
						return null;
					} else avalaibleReplicaServer = false;
				}
			} else {
				System.err.println("#readFileRequest : Chunk hundle for chunk number "
						+ chunk + " could not be found for file " + fileName);
				return null;
			}
		}
		return result;
	}

	@Override
	public ArrayList<String> deleteFileRequest(String fileName) throws RemoteException {
		if (!this.metadata.containsKey(fileName)) {
			System.err.println("#deleteFileRequest : file " + fileName
					+ " unknown to NameNode");
			return null;
		}
		ArrayList<String> result = new ArrayList<String>();
		FileData fileData = this.metadata.get(fileName);
		for(int chunk = 0 ; chunk < fileData.getFileSize()/fileData.getReplicationFactor() ; chunk++) {
			if (fileData.containsChunkHandle(chunk)) {
				for (String server : fileData.getChunkHundle(chunk)) {
					if (!this.avalaibleServers.contains(server)) {
						if (!result.contains(server)) {
							System.err.println("#deleteFileRequest : server " + server 
									+ " is not avalaible, could not delete chunk(s)"
									+ " from file " + fileName);
						}
					} else result.add(server);
				}
			} else {
				System.err.println("#deleteFileRequest : Chunk hundle for chunk number "
						+ chunk + " could not be found for file " + fileName);
			}
		}
		return result;
	}

	@Override
	public void chunkWriten(String fileName, int fileSize, int chunkSize, int replicationFactor, int chunkNumber, String server) 
			throws RemoteException {
		FileData fileData;
		if (!this.metadata.containsKey(fileName)) {
			fileData = new FileData(fileSize, chunkSize, replicationFactor); //FIXME file size
			fileData.addChunkLocation(chunkNumber, server);
			this.metadata.put(fileName, fileData); //FIXME order of chunks number
		} else {
			fileData = this.metadata.get(fileName);
			if (fileData.getChunkSize() != chunkSize) {
				if (fileData.getFileSize() != fileSize || fileData.getReplicationFactor() != replicationFactor) {
					fileData.setFileSize(fileSize);
					fileData.setChunkSize(chunkSize);
					fileData.setReplicationFactor(replicationFactor);
					//FIXME On garde les anciens chunk ou pas?
					System.err.println("#warning : data for file "
							+ fileName + " has been overwritten by new ones");
				} else {
					fileData.setChunkSize(chunkSize);
					System.out.println(">>>Map result of chunk "
							+ chunkNumber + " from file " + fileName
							+ "stored on server " + server);
				}
			}
			fileData.addChunkLocation(chunkNumber, server); //FIXME order of chunks number
		}
		//Run data writing in backup file
		(new Thread(this.dataWriter)).start();
		this.printMetaData();
	}
	
	@Override
	public void allChunkWriten(String fileName) {
		FileData fileData;
		if (!this.metadata.containsKey(fileName)) { //Empty file
			fileData = new FileData(0, 0, 1); //FIXME file size
			this.metadata.put(fileName, fileData); //FIXME order of chunks number
		} else {
			fileData = metadata.get(fileName);
			fileData.setFileSize(fileData.getChunkHandles().size());
		}
	}

	/**
	 * Load NameNode data from an existing backup local file
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
					System.err.println("#recoverData : content of backup file "
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
	 * Prints metadata on output stream
	 */
	private void printMetaData() {
		System.out.println(">>> PRINTING DATA");
		for (String file : this.metadata.keySet()) {
			System.out.println("$" + file);
			System.out.println("file size : "+this.metadata.get(file).getFileSize()
					+ ", chunk size : " + this.metadata.get(file).getChunkSize()
					+ ", replication factor : " + this.metadata.get(file).getReplicationFactor());
			for (int chunkHandle : this.metadata.get(file).getChunkHandles().keySet()) {
				System.out.println("chunk "+chunkHandle+" : "+this.metadata.get(file).getChunkHundle(chunkHandle));
			}
		}
	}

	/**
	 * Main
	 * Initializes a NameNodeImpl instance and bounds it to the RMI registry
	 * @param args
	 */
	public static void main(String[] args) {
		(new File(backupFile)).delete();
		try {
			System.out.println(InetAddress.getLocalHost().getHostAddress());
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		long fileLength = (new File("data/filesample/filesample.txt").length());
		System.out.println("file length : "+fileLength);
		System.out.println("fileLength % Project.CHUNK_SIZE : " + fileLength % Project.CHUNK_SIZE);
		System.out.println("fileLength / Project.CHUNK_SIZE : " + fileLength / Project.CHUNK_SIZE);
		int fileSize = fileLength % Project.CHUNK_SIZE > 0 ? (int) (fileLength / Project.CHUNK_SIZE + 1)
				: (int) (fileLength / Project.CHUNK_SIZE);
		System.out.println(fileSize);


		try{
			LocateRegistry.createRegistry(Project.PORT_NAMENODE);
		} catch(Exception e) {
		}
		try {
			Naming.rebind("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode", new NameNodeImpl());
			System.out.println(">>> NameNode bound in registry");
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}
}