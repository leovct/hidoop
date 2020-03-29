package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import config.Project;
import config.Project.Commande;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;


/**
 * Implémentation de la classe HdfsClient du servce Hdfs
 */
public class HdfsClient {
	/**
	 * ArrayList<Integer> serverOfChunk :
	 * Le chunk i se trouve sur le serveur chunks.get(i)
	 */
	static ArrayList<Integer> serverOfChunk = new ArrayList<Integer>();
	private static final int bufferSize = 4096;
	private static String tagHdfsClientWrite = "-clientlocalwritechunk";
	private static String tagHdfsClientRead = "-clientlocalreadchunk";
	private static final String messageHeaderError = "# HdfsClient READ  : Message header received"
			+ "is incorrect or non-existent\nExpected :\n - CMD_READ Command (Commande object)"
			+ "\n - File name (String object)\n - File extension (String object)"
			+ "\n - Chunk Number (Integer object)";
	private static final String missingChunksError = "# HdfsClient READ  : Could not build"
			+ " original file : at least one chunk has not been received";
	private static final String buildingFileError = "# HdfsClient READ  : Error occured while"
			+ " building original file";


	/**
	 * Ecrit un fichier dans Hdfs :
	 * Le fichier "localFSSourceFname" est découpé en fragments
	 * qui sont envoyés sur les différents serveurs
	 * @param fmt format du fichier écrit
	 * @param localFSSourceFname fichier écrit
	 * @param repFactor facteur de réplication
	 */
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
		String fileName = ((localFSSourceFname.contains(".")) 
				? localFSSourceFname.substring(0,localFSSourceFname.lastIndexOf('.')) : localFSSourceFname),
				fileExtension = localFSSourceFname.substring(fileName.length(), localFSSourceFname.length());
		//String host = Project.HOST[(new Random()).nextInt(Project.NBR_DATANODE)];
		Format input, tempOutput;
		KV structure;
		Socket socket;
		ObjectOutputStream socketOutputStream;
		BufferedInputStream bis;
		byte[] buf = new byte[bufferSize];;
		int nbRead, chunkCounter = 0;
		long index = 0;

		// Instanciation de l'objet Format
		input = instanceFormat(fmt, localFSSourceFname);
		input.open(Format.OpenMode.R);
		System.out.println(">>> [HDFSWRITE]\n"
				+ ">>> Processing file " + localFSSourceFname + "...");
		// Boucle tant que l'entièreté du fichier d'entrée n'a pas été traité
		// Une itération correspond au traitement d'un chunk
		while ((structure = input.read()) != null) {
			System.out.println("OK");
			// Ecriture du chunk dans un fichier local
			tempOutput = instanceFormat(fmt, fileName+tagHdfsClientWrite+chunkCounter+fileExtension);
			tempOutput.open(Format.OpenMode.W);
			tempOutput.write(structure);

			index = input.getIndex();
			while ((input.getIndex() - index <= Project.CHUNKSIZE) && (structure = input.read())!= null) { //Tant qu'on a pas atteint la fin du fichier et qu'on a pas depasse la taille d'un chunk
				tempOutput.write(structure);
			}
			tempOutput.close();
			if (structure != null && structure.v.length() > Project.CHUNKSIZE) 
				throw new RuntimeException("# Error HdfsWrite : Input file contains "
						+ "a structure value whose size is bigger than chunk size ("+Project.CHUNKSIZE+")");

			// Envoi du chunk au serveur - Transmission par socket
			try {
				System.out.println("NOERROR0");
				//socket = new Socket(InetAdress.getLocalHost(), Project.PORT_HDFSSERVEUR);
				socket = new Socket(Project.HOST[chunkCounter%Project.NBR_DATANODE], Project.PORT_HDFSSERVEURASUP[chunkCounter%Project.NBR_DATANODE]); //remplacer le 1er argument par Project.HOST[chunkCounter%Project.NBR_DATANODE]
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				bis = new BufferedInputStream(new FileInputStream(tempOutput.getFname()), bufferSize);
				socketOutputStream.writeObject(Commande.CMD_WRITE);
				socketOutputStream.writeObject(fileName);
				socketOutputStream.writeObject(fileExtension);
				socketOutputStream.writeObject(chunkCounter);
				while((nbRead = bis.read(buf)) != -1) {
					socketOutputStream.write(buf, 0, nbRead);
				}
				socketOutputStream.close();
				socket.close();
				bis.close();
				System.out.println(">>> Chunk n°" + chunkCounter + " sent on server "
						+ Project.HOST[chunkCounter%Project.NBR_DATANODE]);
			} catch (Exception e) {
				e.printStackTrace();
			}
			serverOfChunk.add(chunkCounter%Project.NBR_DATANODE);
			chunkCounter++;
		}
		input.close();
		System.out.println(">>> File " + localFSSourceFname
				+ " : process completed (" + chunkCounter + " chunks)");

		/*
			// On crÃ©Ã© la CFiche Ã  envoyer au NameNode
			CFicheImpl c = new CFicheImpl(localFSSourceFname,node);
			// RÃ©cupÃ©ration du NameNode et de la liste des chunks
			try {
				System.out.println("On rÃ©cupÃ¨re le stub de : //"+Project.NAMENODEHOST+":"+Project.PORT_NAMENODE+"/NameNode");
				NameNode nm = (NameNode)Naming.lookup("//"+Project.NAMENODEHOST+":"+Project.PORT_NAMENODE+"/NameNode");
				nm.Ajouter(c);
    			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		// Message dans la console
		System.out.println("Le fichier " + localFSSourceFname + " a Ã©tÃ© ajoutÃ© dans HDFS avec succÃ¨s (" + nbChunks + " fragments) !");
		 */
	}


	/**
	 * Lit un fichier "hdfsName" dans HDFS :
	 * Les fragments du fichier sont récoltés sur les différents serveurs
	 * puis concaténés dans le fichier "localFSDestFname"
	 * 
	 * @param hdfsFname fichier à lire
	 * @param localFSDestFname fichier destination
	 */
	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		String fileName = ((hdfsFname.contains(".")) 
				? hdfsFname.substring(0,hdfsFname.lastIndexOf('.')) : hdfsFname),
				fileExtension = hdfsFname.substring(fileName.length(), hdfsFname.length());
		Socket socket;
		ObjectInputStream socketInputStream; ObjectOutputStream socketOutputStream;
		BufferedInputStream bis; BufferedOutputStream bos;
		Object objectReceived; File chunkReceived;
		ArrayList<Integer> chunksReceived = new ArrayList<Integer>();
		int nbRead, chunkNumber, chunkCounter = 0;
		byte[] buf = new byte[bufferSize];

		for (int server = 0 ; server < Project.NBR_DATANODE ; server++) { //server < Project.NBR_DATANODE
			try {
				socket = new Socket(Project.HOST[server], Project.PORT_HDFSSERVEURASUP[server]); //remplacer le 1er argument par Project.HOST[server]
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				socketOutputStream.writeObject(Commande.CMD_READ);
				socketOutputStream.writeObject(fileName);
				socketOutputStream.writeObject(fileExtension);

				socketInputStream = new ObjectInputStream(socket.getInputStream());
				while ((objectReceived = socketInputStream.readObject()) != null) {
					if (objectReceived instanceof Commande && (Commande) objectReceived == Commande.CMD_READ) {
						if ((objectReceived = socketInputStream.readObject()) instanceof String 
								&& ((String) objectReceived).equals(fileName)) {
							if ((objectReceived = socketInputStream.readObject()) instanceof String 
									&& ((String) objectReceived).equals(fileExtension)) {
								if ((objectReceived = socketInputStream.readObject()) instanceof Integer) {
									chunkNumber = (int) objectReceived;
									chunksReceived.add(chunkNumber);
									bos = new BufferedOutputStream(new FileOutputStream(fileName+tagHdfsClientRead+chunkNumber+fileExtension), bufferSize);
									while((nbRead = socketInputStream.read(buf)) != -1) {
										bos.write(buf, 0, nbRead);
									}
									bos.close();
									chunkCounter++;
									System.out.println(">>> Chunk received : "+fileName+tagHdfsClientRead+chunkNumber+fileExtension);
								} else System.err.println(messageHeaderError);
							} else System.err.println(messageHeaderError);
						} else System.err.println(messageHeaderError);
					} else System.err.println(messageHeaderError);
				}
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(">>> End of chunks reception "
				+ "for file " + hdfsFname + "\n"
				+ ">>> Attempting to build original file from chunks...");
		// La variable chunkCounter permet d'éviter des appels couteux à receivedChunks.size()
		System.out.println("chunkCounter : " + chunkCounter + "\n" + chunksReceived);
		if (!checkIntegerSequence(chunksReceived, chunkCounter)) System.err.println(missingChunksError);
		else {
			for (chunkNumber = 0 ; chunkNumber < chunkCounter; chunkNumber++) {
				try {
					bis = new BufferedInputStream(new FileInputStream(fileName+tagHdfsClientRead+chunkNumber+fileExtension), bufferSize);
					bos = new BufferedOutputStream(new FileOutputStream(localFSDestFname, (chunkNumber == 0) ? false : true), bufferSize);
					while((nbRead = bis.read(buf)) != -1) {
						bos.write(buf, 0, nbRead);
					}
					bis.close();
					bos.close();
				} catch (Exception e) {
					System.err.println(buildingFileError);
					e.printStackTrace();
					chunkNumber = chunkCounter;
				}
			}
		}
		// SUPRESSION CHUNK LOCAUX
		for (int chunk : chunksReceived) {
			System.out.println(">>> Attempting to delete " + fileName+tagHdfsClientRead+chunk+fileExtension);
			if ((chunkReceived = new File(fileName+tagHdfsClientRead+chunk+fileExtension)).exists()) {
				chunkReceived.delete();
				System.out.println(">>> OK");
			}
		}
	}


	/**
	 * Permet de supprimer un fichier dans HDFS 
	 * Les fragments du fichier, stockes
	 * sur les differentes machines, sont supprimes
	 */
	public static void HdfsDelete(String hdfsFname) {
		Socket socket;
		ObjectOutputStream socketOutputStream;
		String fileName = ((hdfsFname.contains(".")) 
				? hdfsFname.substring(0,hdfsFname.lastIndexOf('.')) : hdfsFname);
		String fileExtension = hdfsFname.substring(fileName.length(), hdfsFname.length());
		String fileDirectoryName = ((hdfsFname.contains("/")) 
				? hdfsFname.substring(0,hdfsFname.lastIndexOf('/'))+'/' : 
					((hdfsFname.contains("\\")) ? hdfsFname.substring(0,hdfsFname.lastIndexOf('\\'))+'\\' : ""));
		File fileDirectory = new File(fileDirectoryName.equals("") ? "./" : fileDirectoryName);
		System.out.println(">>> [HDFSDELETE]\n"
				+ ">>> Deleting file " + hdfsFname + "from servers...");

		// On envoie un delete a tous les datanodes du systeme HDFS     /// IL FAUT ENVOYER SEULEMENT SUR LES SERV QUI CONTIENNENT UN CHUNK
		for (int server = 0 ; server < Project.NBR_DATANODE ; server++) {
			try {
				//socket = new Socket(host, Project.PORT_HDFSSERVEUR);
				socket = new Socket(Project.HOST[server], Project.PORT_HDFSSERVEURASUP[server]); //remplacer le 1er argument par Project.HOST[server]
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				socketOutputStream.writeObject(Commande.CMD_DELETE);
				socketOutputStream.writeObject(fileName);
				socketOutputStream.writeObject(fileExtension);
				socketOutputStream.close(); // le socketOutputStream est probablement déjà fermé lors de l'appel à socket.close()
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println(">>> Delete command was sent to servers "
				+ "for file " + hdfsFname);
		for (String file : fileDirectory.list()) {
			if (file.startsWith(fileName.substring(fileDirectoryName.length()) + tagHdfsClientWrite) && file.endsWith(fileExtension)) {
				System.out.println(">>> Deleting local chunk : "
						+ fileDirectoryName + file);
				if (!(new File(fileDirectoryName+file).delete()))
					System.out.println(">>> Could not delete chunk : "
							+ fileDirectoryName + file + ", error occured");
			}
		}
		System.out.println(">>> Local chunks deleted "
				+ "for file " + hdfsFname);
	}


	/**
	 * Instancie un objet Format à partir de son type et son nom de fichier
	 * @param fmt
	 * @param fileName
	 * @return
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
	 * Vérifie si une ArrayList<Integer> contient tous les entiers de 0 à listSize-1
	 * @param list la liste contenant les id de chunk (dans ce contexte)
	 * @param listSize taille de la liste
	 * @return true si la liste contient tous les entiers entre 0 et listSize-1
	 */
	static boolean checkIntegerSequence(ArrayList<Integer> list, int listSize) {
		ArrayList<Integer> listCopy = new ArrayList<Integer>(list);
		for (int i = 0 ; i < listSize ; i++) {
			if (list.contains(i)) listCopy.remove((Integer) i);
			else return false;
		}
		return true;
	}


	/**
	 * Affiche l'utilisation de l'application sur le flux de sortie
	 */
	private static void printUsage() {
		System.out.println("[HDFSCLIENT] Incorrect parameters\nUsage :"
				+ "\njava HdfsClient write <line|kv> <file>"
				+ "\njava HdfsClient read <file> <destfile>"
				+ "\njava HdfsClient delete <file>");
	}


	/**
	 * main de l'application
	 * Utilisations prévues : 
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
