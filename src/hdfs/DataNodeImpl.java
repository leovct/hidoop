package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Random;

import config.SettingsManager;
import config.SettingsManager.Command;

public class DataNodeImpl extends UnicastRemoteObject implements DataNode {
	/**
	 * Constants.
	 */
	private static final long serialVersionUID = 1L;
	private static final int bufferSize = 4096;
	private static String messageHeader = ">>> [DATANODE] ";
	private static String errorHeader = ">>> [ERROR] ";
	private static final String receivedMessageHeaderError = errorHeader + "Message header "
			+ "is incorrect or non-existent\nExpected :\n - Command type (Commande object)"
			+ "\n - File name (String object)"
			+ "\n - Chunk Number (Integer object)";
	private static final String chunkNotFoundError = errorHeader + "Couldn't find "
			+ "chunks on this server for file ";
	private static final String nameNodeNotBoundError = errorHeader + "NameNode is not "
			+ "bound in registry, leaving process";
	
	/**
	 * Master NameNode.
	 */
	private NameNode nameNode;

	/**
	 * Address of the server the DataNode is running on.
	 */
	private String serverAddress;

	/**
	 * Runnable class performing operations
	 * when NameNode receives a command
	 * (Nested class)
	 */
	class TaskExecutor implements Runnable {
		private int port;
		private Command command;
		private String fileName;
		private int chunkNumber;
		private ServerSocket serverSocket;

		/**
		 * Constructor..
		 * @param port
		 * @param command
		 * @param fileName
		 * @param chunkNumber
		 */
		public TaskExecutor(int port, Command command, String fileName, int chunkNumber) {
			this.command = command;
			this.port = port;
			this.fileName = fileName;
			this.chunkNumber = chunkNumber;
			try {
				this.serverSocket = new ServerSocket(this.port);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			if (this.command == Command.CMD_WRITE) {
				this.write();
			} else if (this.command == Command.CMD_DELETE) {
				this.delete();
			} else if (this.command == Command.CMD_READ) {
				this.read();
			}
		}

		/**
		 * Write a chunk on server.
		 */
		private void write() {
			Socket communicationSocket, socketPropagateChunkCopy;
			ObjectOutputStream socketOutputStream;
			ObjectInputStream socketInputStream;
			BufferedInputStream bis; BufferedOutputStream bos;
			ArrayList<String> copiesLocations;
			int repFactor, nbRead;
			byte[] buf = new byte[bufferSize];

			try { //Receive additional information : repFactor and copiesLocations
				communicationSocket = this.serverSocket.accept();
				socketInputStream = new ObjectInputStream(communicationSocket.getInputStream());
				repFactor = (int) socketInputStream.readObject();
				copiesLocations = new ArrayList<String>();
				for (int i = 0 ; i < repFactor - 1 ; i++) {
					copiesLocations.add((String) socketInputStream.readObject());
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(receivedMessageHeaderError + "\n - File Size (Integer object)"
						+ "\n - Replication Factor (Integer object)"
						+ "\n - Name of the servers storing chunk copies (String objects, if Replication Factor > 1)");
				return; 
			}
			try { //Receive chunk
				bos = new BufferedOutputStream(new FileOutputStream(
						SettingsManager.DATA_FOLDER+fileName+SettingsManager.TAG_DATANODE+this.chunkNumber), bufferSize);
				while((nbRead = socketInputStream.read(buf)) != -1) { 
					bos.write(buf, 0, nbRead);
				}
				bos.close();
				socketInputStream.close();
				communicationSocket.close();
				this.serverSocket.close();
				System.out.println(messageHeader + "Chunk received : "+fileName+SettingsManager.TAG_DATANODE+this.chunkNumber);
				if (repFactor > 1) { //Propagate chunk
					DataNode dataNode = (DataNode) Naming.lookup("//"+copiesLocations.get(0)+":"+SettingsManager.PORT_DATANODE+"/DataNode");
					socketPropagateChunkCopy = new Socket(copiesLocations.get(0), 
							dataNode.processChunk(Command.CMD_WRITE, fileName, this.chunkNumber));
					socketOutputStream = new ObjectOutputStream(socketPropagateChunkCopy.getOutputStream());
					socketOutputStream.writeObject(repFactor-1);
					for (String server : copiesLocations.subList(1, repFactor-1)) {
						socketOutputStream.writeObject(server);
					}
					bis = new BufferedInputStream(new FileInputStream(
							SettingsManager.DATA_FOLDER+fileName+SettingsManager.TAG_DATANODE+this.chunkNumber), bufferSize);
					while((nbRead = bis.read(buf)) != -1) {
						socketOutputStream.write(buf, 0, nbRead);
					}
					bis.close();
					socketOutputStream.close();
					socketPropagateChunkCopy.close();
					System.out.println(messageHeader + "Chunk sent : n°" + this.chunkNumber + " from file "
							+ this.fileName + " sent to "
							+ copiesLocations.get(0));
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * Read a chunk from server.
		 * Propagate chunk, if replication factor is greater than 1.
		 */
		private void read() {
			ObjectOutputStream socketOutputStream;
			BufferedInputStream bis;
			byte[] buf = new byte[bufferSize];
			int nbRead;
			try {
				Socket communicationSocket = serverSocket.accept();
				socketOutputStream = new ObjectOutputStream(communicationSocket.getOutputStream());
				if ((new File(SettingsManager.DATA_FOLDER+fileName+SettingsManager.TAG_DATANODE+chunkNumber)).exists()) {
					bis = new BufferedInputStream(new FileInputStream(
							SettingsManager.DATA_FOLDER+fileName+SettingsManager.TAG_DATANODE+chunkNumber), bufferSize);
					socketOutputStream.writeObject(Command.CMD_READ);
					socketOutputStream.writeObject(fileName);
					socketOutputStream.writeObject(chunkNumber);
					while((nbRead = bis.read(buf)) != -1) {
						socketOutputStream.write(buf, 0, nbRead);
					}
					bis.close();
					System.out.println(messageHeader + "Chunk n°" + chunkNumber + " from file " + fileName
							+ " sent to client");
				} else System.err.println(chunkNotFoundError 
						+ " : " +SettingsManager.DATA_FOLDER+fileName+SettingsManager.TAG_DATANODE+chunkNumber);
				socketOutputStream.close();
				communicationSocket.close();
				this.serverSocket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * Delete a chunk from server.
		 */
		private void delete() {
			boolean chunkFound = false;
			int chunkNumber;
			for (String file : (new File(SettingsManager.DATA_FOLDER)).list()) {
				if (file.startsWith(fileName+SettingsManager.TAG_DATANODE)) {
					try {
						chunkNumber = Integer.parseInt(file.substring((fileName+SettingsManager.TAG_DATANODE).length(),
								file.length()));
						if ((new File(SettingsManager.DATA_FOLDER+file)).delete()) {
							nameNode.chunkDeleted(fileName, chunkNumber, serverAddress);
							System.out.println(messageHeader + "Chunk deleted : "
									+SettingsManager.DATA_FOLDER+fileName+SettingsManager.TAG_DATANODE+chunkNumber);
						} else System.err.println(errorHeader + "Could not delete file " + file);
					} catch (RemoteException e) {
						System.out.println(nameNodeNotBoundError);
					} catch (NumberFormatException e ) {
						e.printStackTrace();
					} catch (Exception e) {
						e.printStackTrace();
					}
					chunkFound = true;
				}
			}
			if (!chunkFound) System.err.println(chunkNotFoundError + fileName);
		}
	}

	/**
	 * Constructor.
	 * @param nameNode stub for the NameNode of the (unique) cluster this DataNode belongs to
	 * @throws RemoteException
	 */
	protected DataNodeImpl(NameNode nameNode, String serverAddress) throws RemoteException {
		this.nameNode = nameNode;
		this.serverAddress = serverAddress;
		this.nameNode.notifyNameNodeAvailability(this.serverAddress);
	}

	@Override
	public int processChunk(SettingsManager.Command command, String fileName, int chunkNumber) {
		boolean avalaiblePortFound = false;
		ServerSocket testSocket;
		int maxTry = 10000, counter = 0, port = (new Random()).nextInt(63000) + 2000;
		while (!avalaiblePortFound && counter < maxTry) {
			try {
				testSocket = new ServerSocket(port);
				testSocket.close();
				(new Thread(new TaskExecutor(port, command, fileName, chunkNumber))).start();
				avalaiblePortFound = true;
			} catch (IOException ex) {} //Port is occupied
			counter++;
		}
		if (counter == maxTry) return -1;
		else return port;
	}		

	/**
	 * Prints main usage on output stream.
	 */
	private static void printUsage() {
		System.out.println(messageHeader + "Incorrect parameters\nUsage :"
				+ "\njava DataNodeImpl <server>\nWith server = address of the server"
				+ " the DataNodeImpl is executed on");
	}

	/**
	 * Main.
	 * Initializes a DataNodeImpl instance and bounds it to the RMI registry
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 1) {
			System.out.println(messageHeader + "DataNode starting...");
			try{
				LocateRegistry.createRegistry(SettingsManager.PORT_DATANODE);
			} catch(Exception e) {}
			try { //Connection to NameNode and initialization
				NameNode nameNode = (NameNode) Naming.lookup(
						"//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
				Naming.bind("//"+args[0]+":"+SettingsManager.PORT_DATANODE+"/DataNode", new DataNodeImpl(nameNode, args[0]));
				System.out.println(messageHeader + "DataNode bound in registry");
				if (!(new File(SettingsManager.DATA_FOLDER).exists())) {
					(new File(SettingsManager.DATA_FOLDER)).mkdirs(); //Create data directory
				}
			} catch (NotBoundException e) {
				System.err.println(nameNodeNotBoundError);
			} catch (AlreadyBoundException e) {
				System.err.println(errorHeader + "DataNode is already running on this server");
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else printUsage();
	}
}
