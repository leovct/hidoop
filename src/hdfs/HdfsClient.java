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

import config.Project;
import config.Project.Command;
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
	private static final String messageHeaderError = "#HdfsClient READ : Message header received"
			+ "is incorrect or non-existent\nExpected :\n - CMD_READ Command (Commande object)"
			+ "\n - File name (String object)\n - File extension (String object)"
			+ "\n - Chunk Number (Integer object)";
	private static final String readFileError = "#HdfsClient READ : Error occured while"
			+ " reading file on HDFS";
	private static final String nameNodeNotBoundError = "#HdfsClient READ : NameNode is not "
			+ "bound in registry, leaving process";
	private static final String nameNodeServerDoesNotRespondError = "#HdfsClient : NameNode server "
			+ "does not respond";
	private static final String NameNodeFileError = "#HdfsClient : specified file "
			+ "could not be retrieved from NameNode";
	
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
		ArrayList<String> nameNodeResponse;
		String fileName = ((localFSSourceFname.contains(".")) //Separates file name and file extension
				? localFSSourceFname.substring(0,localFSSourceFname.lastIndexOf('.')) : localFSSourceFname),
				fileExtension = localFSSourceFname.substring(fileName.length(), localFSSourceFname.length());
		fileName = ((fileName.contains("/")) //Removes path from file name
				? fileName.substring(fileName.lastIndexOf('/')+1) : 
					((fileName.contains("\\")) ? fileName.substring(fileName.lastIndexOf('\\')+1) : fileName));
		System.out.println(fileName);
		Format input;
		KV structure;
		Socket socket;
		ObjectOutputStream socketOutputStream;
		byte[] buf;
		int chunkCounter = 0;
		long index = 0;

		try { //Connection to NameNode
			nameNode = (NameNode) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode"); 
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
		System.out.println(">>> [HDFSWRITE]\n"
				+ ">>> Processing file " + localFSSourceFname + "...");
		while ((structure = input.read()) != null) {
			try {
				nameNodeResponse = nameNode.writeChunkRequest(repFactor);
				socket = new Socket(nameNodeResponse.get(0), Project.PORT_DATANODE);
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				socketOutputStream.writeObject(Command.CMD_WRITE);
				socketOutputStream.writeObject(fileName);
				socketOutputStream.writeObject(fileExtension);
				socketOutputStream.writeObject(chunkCounter);
				socketOutputStream.writeObject(repFactor);
				for (int i = 1 ; i < repFactor ; i++) {
					socketOutputStream.writeObject(nameNodeResponse.get(i));
				}
				buf = structure.writeSyntax(fmt);
				socketOutputStream.write(buf, 0, buf.length);
				index = input.getIndex();
				while ((input.getIndex() - index <= Project.CHUNK_SIZE) && (structure = input.read())!= null) { 
					buf = structure.writeSyntax(fmt);
					socketOutputStream.write(buf, 0, buf.length);
				}
				socketOutputStream.close();
				socket.close();
				nameNode.chunkWriten(fileName+fileExtension, -1, Project.CHUNK_SIZE, repFactor, 
						chunkCounter, nameNodeResponse.get(0));
				chunkCounter++;
				if (structure != null && structure.v.length() > Project.CHUNK_SIZE) 
					throw new RuntimeException("#Error HdfsWrite : Input file contains "
							+ "a structure value whose size is bigger than chunk size ("+Project.CHUNK_SIZE+")");
				System.out.println(">>> Chunk n°" + chunkCounter + " sent on server "
						+ Project.DATANODES[chunkCounter%Project.NUMBER_OF_DATANODE]);
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
		input.close();
		try {
			nameNode.allChunkWriten(fileName+fileExtension);
		} catch (RemoteException e) {
			e.printStackTrace();
			return;
		}
		System.out.println(">>> File " + fileName+fileExtension
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
		ArrayList<String> nameNodeResponse;
		String fileName = ((hdfsFname.contains(".")) //Separates file name and file extension
				? hdfsFname.substring(0,hdfsFname.lastIndexOf('.')) : hdfsFname),
				fileExtension = hdfsFname.substring(fileName.length(), hdfsFname.length());
		fileName = ((fileName.contains("/")) //Removes path from file name
				? fileName.substring(fileName.lastIndexOf('/')+1) : 
					((fileName.contains("\\")) ? fileName.substring(fileName.lastIndexOf('\\')+1) : fileName));
		Socket socket;
		ObjectInputStream socketInputStream; ObjectOutputStream socketOutputStream;
		BufferedOutputStream bos;
		Object objectReceived;
		int nbRead, chunkNumber, chunkCounter = 0;
		byte[] buf = new byte[bufferSize];

		try { //Connection to NameNode
			nameNode = (NameNode) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode");
			nameNodeResponse = nameNode.readFileRequest(fileName+fileExtension);
			if (nameNodeResponse == null) {
				System.err.println(NameNodeFileError);
				return;
			}
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		for (String server : nameNodeResponse) {
			try {
				socket = new Socket(server, Project.PORT_DATANODE);
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				socketOutputStream.writeObject(Command.CMD_READ);
				socketOutputStream.writeObject(fileName);
				socketOutputStream.writeObject(fileExtension);
				socketOutputStream.writeObject(chunkCounter);
				socketInputStream = new ObjectInputStream(socket.getInputStream());
				while ((objectReceived = socketInputStream.readObject()) != null) {
					if (objectReceived instanceof Command && (Command) objectReceived == Command.CMD_READ
							&& (objectReceived = socketInputStream.readObject()) instanceof String 
							&& ((String) objectReceived).equals(fileName)
							&& (objectReceived = socketInputStream.readObject()) instanceof String 
							&& ((String) objectReceived).equals(fileExtension)
							&& (objectReceived = socketInputStream.readObject()) instanceof Integer) {
						chunkNumber = (int) objectReceived;
						bos = new BufferedOutputStream(new FileOutputStream(
								localFSDestFname, (chunkNumber == 0) ? false : true), bufferSize);
						while((nbRead = socketInputStream.read(buf)) != -1) {
							bos.write(buf, 0, nbRead);
						}
						bos.close();									
						chunkCounter++;
						System.out.println(">>> Received chunk number "
								+chunkNumber);
					} else {
						System.err.println(messageHeaderError);
						System.err.println(readFileError);
						(new File(localFSDestFname)).delete();
					}
				}
				socket.close();
			} catch (Exception e) {
				System.err.println(readFileError);
				e.printStackTrace();
			}
		}
		System.out.println(">>> End of chunks reception "
				+ "for file " + hdfsFname + ", file writen as " + localFSDestFname);
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
		ArrayList<String> nameNodeResponse;
		Socket socket;
		ObjectOutputStream socketOutputStream;
		int chunkCounter = 0;
		String fileName = ((hdfsFname.contains(".")) 
				? hdfsFname.substring(0,hdfsFname.lastIndexOf('.')) : hdfsFname),
				fileExtension = hdfsFname.substring(fileName.length(), hdfsFname.length());
		fileName = ((fileName.contains("/")) //Removes path from file name
				? fileName.substring(fileName.lastIndexOf('/')+1) : 
					((fileName.contains("\\")) ? fileName.substring(fileName.lastIndexOf('\\')+1) : fileName));
		System.out.println(">>> [HDFSDELETE]\n"
				+ ">>> Deleting file " + hdfsFname + "from servers...");


		try { //Connection to NameNode
			nameNode = (NameNode) Naming.lookup("//"+Project.NAMENODE+":"+Project.PORT_NAMENODE+"/NameNode");
			nameNodeResponse = nameNode.deleteFileRequest(fileName+fileExtension);
			if (nameNodeResponse == null) {
				System.err.println(NameNodeFileError);
				return;
			}
		} catch (NotBoundException e) {
			System.err.println(nameNodeNotBoundError);
			return;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		for (String server : nameNodeResponse) {
			try {
				socket = new Socket(server, Project.PORT_DATANODE);
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				socketOutputStream.writeObject(Command.CMD_DELETE);
				socketOutputStream.writeObject(fileName);
				socketOutputStream.writeObject(fileExtension);
				socketOutputStream.writeObject(chunkCounter);
				socketOutputStream.close(); 
				socket.close();
				chunkCounter++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(">>> Delete command was sent to servers "
				+ "for file " + hdfsFname);
	}

	/**
	 * Instantiates a Format object according to type and file name
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
	 * Prints main usage on output stream
	 */
	private static void printUsage() {
		System.out.println("[HDFSCLIENT] Incorrect parameters\nUsage :"
				+ "\njava HdfsClient write <line|kv> <file>"
				+ "\njava HdfsClient read <file> <destfile>"
				+ "\njava HdfsClient delete <file>");
	}

	/**
	 * Main 
	 * Usage :
	 * java HdfsClient write <line|kv> <file>
	 * java HdfsClient read <file> <destfile>
	 * java HdfsClient delete <file>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length < 2 || args.length > 3) {
				printUsage();
			} else {
				switch (args[0]) {
				case "write":
					if (args.length < 3) {
						printUsage();
					}
					else if (args[1].equals("line"))
						HdfsWrite(Format.Type.LINE, args[2], 1);
					else if (args[1].equals("kv"))
						HdfsWrite(Format.Type.KV, args[2], 1);
					else {
						printUsage();
					}
					break;
				case "read":
					if (args.length < 3) {
						printUsage();
					}
					HdfsRead(args[1], args[2]);
					break;
				case "delete":
					HdfsDelete(args[1]);
					break;
				default:
					printUsage();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
