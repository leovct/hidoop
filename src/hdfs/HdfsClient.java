package hdfs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.ConnectException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import config.SettingsManager;
import config.SettingsManager.Command;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;


/**
 * Implementation of an HdfsClient class for the HDFS service.
 * Provides static methods allowing to perform operations of HDFS : 
 * writing, reading and deleting a file.
 */
public class HdfsClient {
	/**
	 * Error messages constants
	 */
	private static String messageHeader = ">>> [HDFSCLIENT] ";
	private static String errorHeader = ">>> [ERROR] ";
	private static final String readFileError = errorHeader + "Error occured while"
			+ " reading file on HDFS";
	private static final String nameNodeNotBoundError = errorHeader + "NameNode is not "
			+ "bound in registry, leaving process";
	private static final String dataNodeNotBoundError = errorHeader + "DataNode is not "
			+ "bound in registry, leaving process";
	private static final String nameNodeServerDoesNotRespondError = errorHeader + "NameNode server "
			+ "does not respond";
	private static final String NameNodeFileError = errorHeader + "Specified file "
			+ "could not be retrieved from NameNode";
	private static final String replicationFactorError = errorHeader + "<replicationfactor> "
			+ "must be a strictly positive integer";
	private static final String dataNodeConnectionError = errorHeader + "Could not establish "
			+ "TCP connection with DataNode";

	/**
	 * Buffer size constant
	 */
	private static final int bufferSize = 4096;

	/**
	 * Writes a file in HDFS.
	 * The file is split in chunks that are sent on the 
	 * servers indicated by the NameNode of the cluster.
	 * 
	 * @param fmt format of the written file
	 * @param localFSSourceFname written file
	 * @param repFactor replication factor
	 */
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
		NameNode nameNode;
		DataNode dataNode;
		ArrayList<String> nameNodeResponse;
		String fileName = ((localFSSourceFname.contains("/")) //Removes path from file name
				? localFSSourceFname.substring(localFSSourceFname.lastIndexOf('/')+1) : 
					((localFSSourceFname.contains("\\")) ? 
							localFSSourceFname.substring(localFSSourceFname.lastIndexOf('\\')+1) : localFSSourceFname));
		Format input;
		KV structure;
		Socket socket;
		ObjectOutputStream socketOutputStream;
		byte[] buf;
		int portNumber, chunkCounter = 0;
		long index = 0;

		System.out.println(messageHeader
				+ "%WRITE% Processing file " + localFSSourceFname + "...");
		try { //Connection to NameNode
			nameNode = (NameNode) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode"); 
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
			return;
		} catch (ConnectException e) {
			System.err.println(nameNodeServerDoesNotRespondError);
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		input = instanceFormat(fmt, localFSSourceFname); //Format object Instantiation according to format type
		input.open(Format.OpenMode.R);
		while ((structure = input.read()) != null) {
			try {
				nameNodeResponse = nameNode.writeChunkRequest(repFactor);
				dataNode = (DataNode) Naming.lookup("//"+nameNodeResponse.get(0)+":"+SettingsManager.PORT_DATANODE+"/DataNode");
				if ((portNumber = dataNode.processChunk(Command.CMD_WRITE, fileName, chunkCounter)) == -1) {
					System.err.println(dataNodeConnectionError);
					return;
				}
				socket = new Socket(nameNodeResponse.get(0), portNumber);
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				socketOutputStream.writeObject(nameNodeResponse.size());
				for (int i = 1 ; i < nameNodeResponse.size() ; i++) {
					socketOutputStream.writeObject(nameNodeResponse.get(i));
				}
				buf = structure.writeSyntax(fmt);
				socketOutputStream.write(buf, 0, buf.length);
				index = input.getIndex();
				while ((input.getIndex() - index <= SettingsManager.CHUNK_SIZE) && (structure = input.read())!= null) { 
					buf = structure.writeSyntax(fmt);
					socketOutputStream.write(buf, 0, buf.length);
				}
				socketOutputStream.close();
				socket.close();
				for (String server : nameNodeResponse) {
					nameNode.chunkWritten(fileName, -1, SettingsManager.CHUNK_SIZE, repFactor, 
							chunkCounter, server);
				}
				if (structure != null && structure.v.length() > SettingsManager.CHUNK_SIZE) 
					System.err.println(errorHeader + "Input file contains "
							+ "a structure value whose size is bigger than chunk size ("+SettingsManager.CHUNK_SIZE+")");
				System.out.println(messageHeader + "Chunk n°" + chunkCounter + " sent on server "
						+ nameNodeResponse.get(0));
				chunkCounter++;
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
		input.close();
		try {
			nameNode.allChunkWritten(fileName);
		} catch (RemoteException e) {
			e.printStackTrace();
			return;
		}
		//new File(localFSSourceFname).delete(); //Deletes local file once process completed
		System.out.println(messageHeader + "File " + fileName
				+ " : process completed (" + chunkCounter + " chunks)");
	}

	/**
	 * Reads a file in HDFS.
	 * Chunks form the file are collected from servers indicated by the 
	 * NameNode of the cluster and then concatenated in a destination file.
	 * 
	 * @param hdfsFname file read
	 * @param localFSDestFname destination file (concatenation of chunks)
	 */
	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		NameNode nameNode;
		DataNode dataNode;
		ArrayList<ArrayList<String>> nameNodeResponse;
		String fileName = ((hdfsFname.contains("/")) //Removes path from file name
				? hdfsFname.substring(hdfsFname.lastIndexOf('/')+1) : 
					((hdfsFname.contains("\\")) ? hdfsFname.substring(hdfsFname.lastIndexOf('\\')+1) : hdfsFname));
		Socket socket;
		ObjectInputStream socketInputStream;
		BufferedOutputStream bos;
		Object objectReceived;
		int nbRead, portNumber, chunkNumber, chunkHandle = 0, chunkCounter = 0;
		byte[] buf = new byte[bufferSize];
		boolean chunkRead = false;

		System.out.println(messageHeader
				+ "%READ% Processing file " + fileName + "...");
		try { //Connection to NameNode
			nameNode = (NameNode) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
			nameNodeResponse = nameNode.readFileRequest(fileName);
			if (nameNodeResponse == null) {
				System.err.println(NameNodeFileError);
				return;
			}
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
			return;
		} catch (ConnectException e) {
			System.err.println(nameNodeServerDoesNotRespondError);
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		for (ArrayList<String> chunkHandles : nameNodeResponse) {
			chunkHandle = 0;
			while (!chunkRead && chunkHandle < chunkHandles.size()) {
				try {
					dataNode = (DataNode) Naming.lookup("//"+chunkHandles.get(chunkHandle)+":"+SettingsManager.PORT_DATANODE+"/DataNode");
					if ((portNumber = dataNode.processChunk(Command.CMD_READ, fileName, chunkCounter)) == -1) {
						System.err.println(dataNodeConnectionError);
						return;
					}
					socket = new Socket(chunkHandles.get(chunkHandle), portNumber);
					socketInputStream = new ObjectInputStream(socket.getInputStream());
					if ((objectReceived = socketInputStream.readObject()) instanceof Command 
							&& (Command) objectReceived == Command.CMD_READ
							&& (objectReceived = socketInputStream.readObject()) instanceof String 
							&& ((String) objectReceived).equals(fileName)
							&& (objectReceived = socketInputStream.readObject()) instanceof Integer) {
						chunkNumber = (int) objectReceived;
						bos = new BufferedOutputStream(new FileOutputStream(
								localFSDestFname, (chunkNumber == 0) ? false : true), bufferSize);
						while((nbRead = socketInputStream.read(buf)) != -1) {
							bos.write(buf, 0, nbRead);
						}
						bos.close();
						chunkRead = true;
						System.out.println(messageHeader + "Received chunk number "
								+chunkNumber);
					} else {
						System.err.println(errorHeader + "Could not retrieve chunk from " + chunkHandles.get(chunkHandle)
						+ ", trying to retrieve a copy from another server...");
					}
					socketInputStream.close();
					socket.close();
				} catch (Exception e) {
					System.err.println(errorHeader + "A problem occured while trying to retrieve chunk from " 
							+ chunkHandles.get(chunkHandle) + ", trying to retrieve a copy from another server...");
				}
				chunkHandle++;
			}
			if (!chunkRead) {
				System.err.println(readFileError);
				(new File(localFSDestFname)).delete();
				return;
			}
			chunkCounter++;
			chunkRead = false;
		}
		System.out.println(messageHeader + "End of chunks reception "
				+ "for file " + fileName + ", result file writen as " + localFSDestFname);
	}

	/**
	 * Deletes a file in HDFS.
	 * Chunks of the file are deleted on servers 
	 * indicated by the NameNode of the cluster.
	 * 
	 * @param hdfsFname file deleted
	 */
	public static void HdfsDelete(String hdfsFname) {
		NameNode nameNode;
		DataNode dataNode;
		ArrayList<String> nameNodeResponse;
		int chunkCounter = 0;
		String fileName = ((hdfsFname.contains("/")) //Removes path from file name
				? hdfsFname.substring(hdfsFname.lastIndexOf('/')+1) : 
					((hdfsFname.contains("\\")) ? hdfsFname.substring(hdfsFname.lastIndexOf('\\')+1) : hdfsFname));

		System.out.println(messageHeader
				+ "%DELETE% Deleting file " + fileName + "...");
		try { //Connection to NameNode
			nameNode = (NameNode) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
			nameNodeResponse = nameNode.deleteFileRequest(fileName);
			if (nameNodeResponse == null) {
				System.err.println(NameNodeFileError);
				return;
			}
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
			return;
		} catch (ConnectException e) {
			System.err.println(nameNodeServerDoesNotRespondError);
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		for (String server : nameNodeResponse) {
			try {
				dataNode = (DataNode) Naming.lookup("//"+server+":"+SettingsManager.PORT_DATANODE+"/DataNode");
				dataNode.processChunk(Command.CMD_DELETE, fileName, chunkCounter);
			} catch (Exception e) {
				System.out.println(dataNodeNotBoundError + " : " + server);
			}
			chunkCounter++;
		}
		System.out.println(messageHeader + "Delete command was sent to servers "
				+ "for file " + fileName);
	}

	/**
	 * Notifies NameNode a file has been written on a server.
	 * In a first implementation, the aim of this method is to allow
	 * applications from MapReduce to communicate with NameNode.
	 * 
	 * @param fileName name of the file the chunk is part of
	 * @param fileSize size of the file 
	 * @param chunkSize size of the chunk
	 * @param replicationFactor replication factor of the chunk
	 * @param chunkNumber number of the chunk in the file
	 * @param server server containing the chunk
	 * @return boolean, true if success
	 * @throws RemoteException
	 */
	public static boolean notifyNameNode(String fileName, int fileSize, int chunkSize, int replicationFactor, int chunkNumber, String server) {
		try { //Connection to NameNode
			NameNode nameNode = (NameNode) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.PORT_NAMENODE+"/NameNode");
			nameNode.chunkWritten(fileName, fileSize, chunkSize, replicationFactor, chunkNumber, server);
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Instantiates a Format object according to type and file name.
	 * 
	 * @param fmt format
	 * @param fileName file name
	 * @return Format object 
	 * @throws RuntimeException
	 */
	private static Format instanceFormat(Format.Type fmt, String fileName) throws RuntimeException {
		if (fmt == Format.Type.LINE) {
			return new LineFormat(fileName);
		}
		else if (fmt == Format.Type.KV) {
			return new KVFormat(fileName);
		}
		else throw new RuntimeException("instanceFormat : Unsupported input format");
	}

	/**
	 * Prints main usage on output stream.
	 */
	private static void printUsage() {
		System.out.println(messageHeader + "Incorrect parameters\nUsage :"
				+ "\njava HdfsClient write <line|kv> <file> <replicationfactor>"
				+ "\njava HdfsClient read <file> <destfile>"
				+ "\njava HdfsClient delete <file>");
	}

	/**
	 * Main.
	 * Usage :
	 * java HdfsClient write <line|kv> <file> <replicationfactor>
	 * java HdfsClient read <file> <destfile>
	 * java HdfsClient delete <file>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length > 1 && args.length < 5) {
			switch (args[0]) {
			case "write":
				if (args.length > 2) {
					int replicationFactor = 1;
					if (args.length > 3) {
						try {
							if (!((replicationFactor = Integer.parseInt(args[3])) > 0)) {
								System.out.println(replicationFactorError);
								printUsage();
								return;
							}
						} catch (NumberFormatException e) {
							System.out.println(replicationFactorError);
							printUsage();
							return;
						}
					}
					if ((new File(args[2])).exists()) {
						switch (args[1]) {
						case "line":
							HdfsWrite(Format.Type.LINE, args[2], replicationFactor);
							break;
						case "kv":
							HdfsWrite(Format.Type.KV, args[2], replicationFactor);
							break;
						default:
							printUsage();
						}
					} else System.err.println(errorHeader + "File "+args[2]+" could not be found");
				} else printUsage();
				break;
			case "read":
				if (args.length != 3) printUsage();
				else HdfsRead(args[1], args[2]);
				break;
			case "delete":
				if (args.length != 2) printUsage();
				else HdfsDelete(args[1]);
				break;
			default:
				printUsage();
			}
		} else printUsage();
	}
}