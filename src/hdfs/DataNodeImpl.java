package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import config.Project;
import config.Project.Command;

public class DataNodeImpl extends UnicastRemoteObject implements DataNode, Runnable {
	private static final long serialVersionUID = 1L;

	/**
	 * Master NameNode
	 */
	private NameNode nameNode;

	/**
	 * Constants
	 */
	private static final int bufferSize = 100;
	private static String tagDataNode = "-serverchunk";
	private static final String messageHeaderError = "# HdfsServeur Error : Message header "
			+ "is incorrect or non-existent\nExpected :\n - Command type (Commande object)"
			+ "\n - File name (String object)\n - File extension (String object)"
			+ "\n - Chunk Number (Integer object)";
	private static final String chunkNotFoundError = "# HdfsServeur Error : Couldn't find required "
			+ "chunk on this server";
	private static final String nameNodeNotBoundError = "# HdfsClient READ  : NameNode is not "
			+ "bound in registry, leaving process";

	/**
	 * Runnable class performing operations
	 * when NameNode receives a command
	 * (Nested class)
	 */
	class TaskExecutor implements Runnable {
		private Command command;
		public TaskExecutor(Command command) {
			this.command = command;
		}

		@Override
		public void run() {
			if (this.command == Command.CMD_WRITE) {

			}
		}
	}

	/**
	 * Constructor
	 * @param nameNode stub for the NameNode of the (unique) cluster this DataNode belongs to
	 * @throws RemoteException
	 */
	protected DataNodeImpl(NameNode nameNode) throws RemoteException {
		this.nameNode = nameNode;
	}

	/**
	 * Notifier le NameNode de l'ajout d'un chunk au serveur
	 * 
	 * @param fileName le nom du fichier d'où est issu le chunk
	 * @param chunkName le nom du chunk sur le serveur
	 * @return un booleen, true en cas de réussite
	 */
	public boolean notifyNameNode(String fileName, String chunkName) throws RemoteException {
		return true;
	}

	@Override
	public void run() {
		try {
			// Creation du serveur
			ObjectInputStream socketInputStream;
			ObjectOutputStream socketOutputStream;
			BufferedInputStream bis;
			BufferedOutputStream bos;
			Command command = null;
			String fileName = "", fileExtension = "";
			Socket communicationSocket, socketPropagateChunkCopy;
			ServerSocket serverSocket = new ServerSocket(Project.PORT_DATANODE);
			byte[] buf = new byte[bufferSize], serverCommandBuf = new byte[1];
			int nbRead, repFactor, chunkNumber = -1;
			boolean serverStop = false;
			ArrayList<String> copiesLocations = new ArrayList<String>();

			System.out.println(">>> Server initialisation...");
			while (!serverStop) {
				// Attente d'une connexion
				System.out.println(">>> [DATANODE] Awaiting a connection");
				communicationSocket = serverSocket.accept();
				System.out.println(">>> Connection received");

				// Reception de la requete du client (commande et nom du fichier a traiter)
				socketInputStream = new ObjectInputStream(communicationSocket.getInputStream());
				try {
					command = (Command) socketInputStream.readObject();
					fileName = (String) socketInputStream.readObject();
					fileExtension = (String) socketInputStream.readObject();
					chunkNumber = (int) socketInputStream.readObject();
					System.out.println(">>> Client's request : " + command + " " 
							+ fileName+fileExtension + " chunk " + chunkNumber);
				} catch (Exception e) {
					System.err.println(messageHeaderError);
				}

				if (command == Command.CMD_WRITE) {
					try {
						//fileSize = (int) socketInputStream.readObject();
						repFactor = (int) socketInputStream.readObject();
						for (int i = 0 ; i < repFactor - 1 ; i++) {
							copiesLocations.add((String) socketInputStream.readObject());
						}
					} catch (Exception e) {
						System.err.println(messageHeaderError + "\n - File Size (Integer object)"
								+ "\n - Replication Factor (Integer object)"
								+ "\n - Name of the servers storing chunk copies (String objects, if Replication Factor > 1)");
						e.printStackTrace();
						break; //FIXME sortie du if si mauvaise réception
					}
					bos = new BufferedOutputStream(new FileOutputStream(
							Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension), bufferSize);
					while((nbRead = socketInputStream.read(buf)) != -1) {
						bos.write(buf, 0, nbRead);
					}
					bos.close();

					for (String server : copiesLocations) {
						try {
							socketPropagateChunkCopy = new Socket(server, Project.PORT_DATANODE);
							socketOutputStream = new ObjectOutputStream(socketPropagateChunkCopy.getOutputStream());
							bis = new BufferedInputStream(new FileInputStream(
									Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension), bufferSize);
							socketOutputStream.writeObject(Command.CMD_WRITE);
							socketOutputStream.writeObject(fileName);
							socketOutputStream.writeObject(fileExtension);
							socketOutputStream.writeObject(chunkNumber);
							socketOutputStream.writeObject(repFactor);
							while((nbRead = bis.read(buf)) != -1) {
								socketOutputStream.write(buf, 0, nbRead);
							}
							socketOutputStream.close();
							socketPropagateChunkCopy.close();
							bis.close();
							System.out.println(">>> Chunk n°" + chunkNumber + " sent on server "
									+ server);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					System.out.println(">>> Chunk received : "+Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension);
				}

				else if (command == Command.CMD_DELETE) {
					if ((new File(Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension).delete())) 
						System.out.println(">>> Chunk deleted : "+Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension);
					else System.err.println(chunkNotFoundError 
							+ " : " +Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension);
				}

				else if (command == Command.CMD_READ) {
					socketOutputStream = new ObjectOutputStream(communicationSocket.getOutputStream());
					if ((new File(Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension)).exists()) {
						try {
							bis = new BufferedInputStream(new FileInputStream(Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension), bufferSize);
							socketOutputStream.writeObject(Command.CMD_READ);
							socketOutputStream.writeObject(fileName);
							socketOutputStream.writeObject(fileExtension);
							socketOutputStream.writeObject(chunkNumber);
							while((nbRead = bis.read(buf)) != -1) {
								socketOutputStream.write(buf, 0, nbRead);
							}
							bis.close();
							System.out.println(">>> Chunk n°" + chunkNumber + " from file " + fileName + fileExtension
									+ " sent to client");
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else System.err.println(chunkNotFoundError 
							+ " : " +Project.DATANODE_FILES_PATH+fileName+tagDataNode+chunkNumber+fileExtension);
					try {
						socketOutputStream.writeObject(null); //Signal that transmission is complete
					} catch (Exception e) {
						e.printStackTrace();
					}
					socketOutputStream.close();
				}
				socketInputStream.close();
				communicationSocket.close();

				//Quit server
				if (System.in.available() > 0) {
					if (System.in.read(serverCommandBuf) == 1) {
						if ((new String(serverCommandBuf)).equals("q")) serverStop = true;
						else System.out.println(">>> [DATANODE] Entry detected, type 'q' then Enter to exit");
					}
					while (System.in.available()>0) System.in.read();
				}
			}
			serverSocket.close(); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Main
	 * Initializes a DataNodeImpl instance and bounds it to the RMI registry
	 * @param args
	 */
	public static void main(String[] args) {
		try { //Connection to NameNode
			NameNode nameNode = (NameNode) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode");
			(new Thread(new DataNodeImpl(nameNode))).start();
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
}
