package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;

import config.Project;
import config.Project.Commande;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;


/**
 * Classe permettant d'Ã©crire, de dÃ©truire et de lire des fichiers dans HDFS
 */
public class HdfsClientRecuperation {

	/**
	 * ArrayList<Integer> serverOfChunk :
	 * Le chunk i se trouve sur le serveur chunks.get(i)
	 */
	static ArrayList<Integer> serverOfChunk = new ArrayList<Integer>();
	private static final int bufferSize = 4096;
	private static String tagHdfsClientWrite = "CLIENTWRITE";
	private static String tagHdfsClientRead = "CLIENTREAD";
	private static final String messageHeaderError = "# HdfsClient READ  : Message header received"
			+ "is incorrect or non-existent\nExpected :\n - CMD_READ Command (Commande object)"
			+ "\n - File name (String object)\n - File extension (String object)"
			+ "\n - Chunk Number (Integer object)";
	private static final String missingChunksError = "# HdfsClient READ  : Could not build"
			+ " original file : at least one chunk has not been received";
	/**
	 * Permet d'ecrire un fichier dans HDFS Le fichier "localFSSourceFname" est
	 * decoupe en fragments qui sont envoyes sur les differentes machines pour y
	 * etre stockes. Le facteur de replication est de 1 dans cette premiere version
	 */
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
		String fileName = ((localFSSourceFname.contains(".")) 
				? localFSSourceFname.substring(0,localFSSourceFname.lastIndexOf('.')) : localFSSourceFname);
		String fileExtension = localFSSourceFname.substring(fileName.length(), localFSSourceFname.length());
		//String host = Project.HOST[(new Random()).nextInt(Project.NBR_DATANODE)];
		Format input, tempOutput;
		KV structure;
		int nbRead, chunkCounter = 0;
		long index = 0;
		Socket socket;
		ObjectOutputStream socketOutputStream;
		BufferedInputStream bis;
		byte[] buf = new byte[bufferSize];;

		// Instanciation de l'objet Format
		input = instanceFormat(fmt, localFSSourceFname);
		input.open(Format.OpenMode.R);
		System.out.println(">>> [HDFSWRITE]\n"
				+ ">>> Processing file " + localFSSourceFname + "...");
		// Boucle tant que l'entièreté du fichier d'entrée n'a pas été traité
		// Une itération correspond au traitement d'un chunk
		while ((structure = input.read()) != null) {
			// Ecriture du chunk dans un fichier local
			tempOutput = instanceFormat(fmt, fileName+tagHdfsClientWrite+chunkCounter+fileExtension);
			tempOutput.open(Format.OpenMode.W);
			tempOutput.write(structure);
			//System.out.println(structure);

			index = input.getIndex();
			while ((input.getIndex() - index <= Project.CHUNKSIZE) && (structure = input.read())!= null) { //Tant qu'on a pas atteint la fin du fichier et qu'on a pas depasse la taille d'un chunk
				//System.out.println(structure);
				tempOutput.write(structure);
			}
			tempOutput.close();
			// if (structure != null) System.out.println(structure.v + " : " +structure.v.length());
			if (structure != null && structure.v.length() > Project.CHUNKSIZE) 
				throw new RuntimeException("# Error HdfsWrite : Input file contains "
						+ "a structure value whose size is bigger than chunk size ("+Project.CHUNKSIZE+")");

			// Envoi du chunk au serveur - Transmission par socket
			try {
				//socket = new Socket(host, Project.PORT_HDFSSERVEUR);
				socket = new Socket(InetAddress.getLocalHost(), Project.PORT_HDFSSERVEUR); //remplacer le 1er argument par Project.HOST[chunkCounter%Project.NBR_DATANODE]
				socketOutputStream = new ObjectOutputStream(socket.getOutputStream());
				bis = new BufferedInputStream(new FileInputStream(tempOutput.getFname()), bufferSize);
				socketOutputStream.writeObject(Commande.CMD_WRITE);
				socketOutputStream.writeObject(fileName);//tempOutput.getFname());
				socketOutputStream.writeObject(fileExtension);
				socketOutputStream.writeObject(chunkCounter);
				//System.out.println("[SOCKET]");
				while((nbRead = bis.read(buf)) != -1) {
					socketOutputStream.write(buf, 0, nbRead);
					//System.out.print(new String(Arrays.copyOfRange(buf, 0, nbRead), "UTF-8"));
					//System.out.print ("#[BUF SIZE]:" + nbRead+"#");
				}
				//System.out.println("\n[FIN SOCKET]");
				socketOutputStream.close(); // le socketOutputStream est probablement déjà fermé lors de l'appel à socket.close()
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
				+ " was processed successfully (" + chunkCounter + " chunks)");

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
					((hdfsFname.contains("\\")) ? hdfsFname.substring(0,hdfsFname.lastIndexOf('\\'))+'\\' : "/"));
		File fileDirectory = new File(fileDirectoryName);
		System.out.println(">>> [HDFSDELETE]\n"
				+ ">>> Deleting file " + hdfsFname + "from servers...");

		// On envoie un delete a tous les datanodes du systeme HDFS     /// IL FAUT ENVOYER SEULEMENT SUR LES SERV QUI CONTIENNENT UN CHUNK
		for (int server = 0 ; server < Project.NBR_DATANODE ; server++) {
			try {
				//socket = new Socket(host, Project.PORT_HDFSSERVEUR);
				socket = new Socket(InetAddress.getLocalHost(), Project.PORT_HDFSSERVEUR); //remplacer le 1er argument par Project.HOST[server]
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
			//System.out.println("indirectory : " + file);
			//System.out.println("startswith? : " + fileName.substring(fileDirectoryName.length()) + tagHdfsClient);
			if (file.startsWith(fileName.substring(fileDirectoryName.length()) + tagHdfsClientWrite) && file.endsWith(fileExtension)) {
				//System.out.println("deleting "+fileDirectoryName+file);
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
	//
	//	/**
	//	 * Permet de lire un fichier "hdfsName" dans HDFS Les fragments du fichier sont
	//	 * recuperes a partir des differentes machines, concatenes et stockes localement
	//	 * dans le fichier "localFSDestFname"
	//	 * 
	//	 * @param hdfsFname
	//	 * @param localFSDestFname
	//	 * @throws IOException
	//	 * @throws UnknownHostException
	//	 */
	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
		Socket socket;
		ObjectInputStream socketInputStream;
		ObjectOutputStream socketOutputStream;
		BufferedInputStream bis;
		BufferedOutputStream bos;
		Object objectReceived;
		int nbRead, chunkNumber, chunkCounter = 0;
		byte[] buf = new byte[bufferSize];
		ArrayList<Integer> receivedChunks = new ArrayList<Integer>();
		String fileName = ((hdfsFname.contains(".")) 
				? hdfsFname.substring(0,hdfsFname.lastIndexOf('.')) : hdfsFname);
		String fileExtension = hdfsFname.substring(fileName.length(), hdfsFname.length());

		for (int server = 0 ; server < 1 ; server++) { //server < Project.NBR_DATANODE
			try {
				socket = new Socket(InetAddress.getLocalHost(), Project.PORT_HDFSSERVEUR); //remplacer le 1er argument par Project.HOST[server]
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
									receivedChunks.add(chunkNumber);
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
		System.out.println("chunkCounter : " + chunkCounter + ":" + receivedChunks);
		if (!checkIntegerSequence(receivedChunks, chunkCounter)) System.err.println(missingChunksError);
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
					e.printStackTrace();
				}
			}
		}
	}


	/**
	 * Permet de vérifier si une liste contient tous les entiers de 0 à listSize-1
	 * @param list la liste contenant les id de chunk (dans ce contexte)
	 * @param listSize taille de la liste
	 * @return true si la liste contient tous les entiers entre 0 et listSize-1
	 */
	static boolean checkIntegerSequence(ArrayList<Integer> list, int listSize) {
		for (int i = 0 ; i < listSize ; i++) {
			if (list.contains(i)) list.remove(i);
			else return false;
		}
		return true;
	}


	/**
	 * Main permettant d'executer les commandes via le terminal
	 */
	public static void main(String[] args) {

		// NOTE : PASSER LE CLIENT EN LIGNE DE COMMANDE COMME LE SERVEUR? $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

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
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Rappel sur l'utilisation de HdfsClient
	 */
	private static void printUsage() {
		System.out.println("Usage: java HdfsClient read <file> <destfile>"
				+"\nUsage: java HdfsClient write <line|kv> <file>"
				+"\nUsage: java HdfsClient delete <file>");
	}

	private static Format instanceFormat(Format.Type fmt, String fileName) throws RuntimeException {
		if (fmt == Format.Type.LINE) {
			return new LineFormat(fileName);
		}
		else if (fmt == Format.Type.KV) {
			return new KVFormat(fileName);
		}
		else throw new RuntimeException("instanceFormat : Unsupported input format");
	}
}
