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
import java.util.ArrayList;
import java.util.HashMap;

import config.Project;
import config.Project.Commande;


/**
 * Implémentation de la classe HdfsServer du servce Hdfs
 */
public class HdfsServer1 extends Thread {
	/**
	 *  Hashmap locale au serveur :
	 *  - String : key - ensembles des noms de fichiers dont au moins 1 chunk a été reçu
	 *  - ArrayList<Integer> : value - numéros des chunk reçus pour chaque fichier
	 */
	static HashMap<String, ArrayList<Integer>> chunks = new HashMap<String, ArrayList<Integer>>();
	private static final int bufferSize = 100;
	private static String tagHdfsServer = "-server1chunk";
	private static final String messageHeaderError = "# HdfsServeur Error : Message header "
			+ "is incorrect or non-existent\nExpected :\n - Command type (Commande object)"
			+ "\n - File name (String object)\n - File extension (String object)";
	private static final String unknownFileNote = ">>> HdfsServeur Note : File requested "
			+ "does not exist on this server";
	private static final String chunkNotFoundError = "# HdfsServeur Error : Couldn't find chunk "
			+ "on this server anymore";

	/**
	 * main de l'application
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// Creation du serveur
			ObjectInputStream socketInputStream;
			ObjectOutputStream socketOutputStream;
			BufferedInputStream bis;
			BufferedOutputStream bos;
			Commande command = null;
			String fileName = "", fileExtension = "";
			Socket communicationSocket;
			ServerSocket serverSocket = new ServerSocket(Project.PORT_HDFSSERVEURASUP[1]);
			byte[] buf = new byte[bufferSize], serverCommandBuf = new byte[1];
			int nbRead, chunkNumber = -1;
			boolean serverStop = false;

			System.out.println(">>> Server initialisation...");
			while (!serverStop) {
				// Attente d'une connexion
				System.out.println(">>> [HDFSSERVER] Awaiting a connection");
				communicationSocket = serverSocket.accept();
				System.out.println(">>> Connection received");

				// Reception de la requete du client (commande et nom du fichier a traiter)
				socketInputStream = new ObjectInputStream(communicationSocket.getInputStream());
				try {
					command = (Commande) socketInputStream.readObject();
					fileName = (String) socketInputStream.readObject();
					fileExtension = (String) socketInputStream.readObject();
					System.out.println(">>> Client's request : " + command + " " + fileName+fileExtension);
				} catch (Exception e) {
					System.err.println(messageHeaderError);
				}

				/// CMD_WRITE
				/////// £££££££££££££££££££££££££ NOTE : VOIR POUR RECEVOIR SEULEMENT LE NOM DE FICHIER SANS LE DOSSIER?
				if (command == Commande.CMD_WRITE) {
					try {
						chunkNumber = (int) socketInputStream.readObject();
					} catch (Exception e) {
						System.err.println(messageHeaderError + "\n - Chunk Number (Integer object)");
						e.printStackTrace();
					}
					bos = new BufferedOutputStream(new FileOutputStream(fileName+tagHdfsServer+chunkNumber+fileExtension), bufferSize);
					while((nbRead = socketInputStream.read(buf)) != -1) {
						bos.write(buf, 0, nbRead);
					}
					bos.close();
					// Ajout de l'information de réception dans la HashMap
					if (!chunks.containsKey(fileName+fileExtension)) chunks.put(fileName+fileExtension, new ArrayList<Integer>());
					if (!chunks.get(fileName+fileExtension).contains(chunkNumber)) chunks.get(fileName+fileExtension).add(chunkNumber);
					System.out.println(">>> Chunk received : "+fileName+tagHdfsServer+chunkNumber+fileExtension);
				}

				else if (command == Commande.CMD_DELETE) {
					// Attention : si le serveur est relancé, il aura oublié les fichiers qu'il connaît
					if (!chunks.containsKey(fileName+fileExtension)) System.out.println(unknownFileNote);
					else {
						for (int chunk : chunks.get(fileName+fileExtension)) {
							if ((new File(fileName+tagHdfsServer+chunk+fileExtension).delete())) 
								System.out.println(">>> Chunk deleted : "+fileName+tagHdfsServer+chunk+fileExtension);
							else System.err.println(chunkNotFoundError 
									+ " : " +fileName+tagHdfsServer+chunk+fileExtension);
						}
						chunks.remove(fileName+fileExtension);
						System.out.println(">>> All chunks from " + fileName + fileExtension
								+ " have been deleted from this server");
					}
				}

				else if (command == Commande.CMD_READ) {
					socketOutputStream = new ObjectOutputStream(communicationSocket.getOutputStream());
					// Attention : si le serveur est relancé, il aura oublié les fichiers qu'il connaît
					if (!chunks.containsKey(fileName+fileExtension)) System.out.println(unknownFileNote);
					else {
						for (int chunk : chunks.get(fileName+fileExtension)) {
							if ((new File(fileName+tagHdfsServer+chunk+fileExtension)).exists()) {
								try {
									bis = new BufferedInputStream(new FileInputStream(fileName+tagHdfsServer+chunk+fileExtension), bufferSize);
									socketOutputStream.writeObject(Commande.CMD_READ);
									socketOutputStream.writeObject(fileName);//tempOutput.getFname());
									socketOutputStream.writeObject(fileExtension);
									socketOutputStream.writeObject(chunk);
									while((nbRead = bis.read(buf)) != -1) {
										socketOutputStream.write(buf, 0, nbRead);
									}
									bis.close();
									System.out.println(">>> Chunk n°" + chunk + " from file " + fileName + fileExtension
											+ "sent to client " + Project.NAMENODEHOST);
								} catch (Exception e) {
									e.printStackTrace();
								}
							} 
							else System.err.println(chunkNotFoundError 
									+ " : " +fileName+tagHdfsServer+chunk+fileExtension);
						}
					}
					try {
						socketOutputStream.writeObject(null); // On signale que la communication est terminée
					} catch (Exception e) {
						e.printStackTrace();
					}
					socketOutputStream.close();
				}
				socketInputStream.close();
				communicationSocket.close();

				// Arrêt du serveur
				if (System.in.available() > 0) {
					if (System.in.read(serverCommandBuf) == 1) {
						if ((new String(serverCommandBuf)).equals("q")) serverStop = true;
						else System.out.println(">>> [HDFSSERVER] Entry detected, type 'q' then Enter to exit");
					}
					while (System.in.available()>0) System.in.read();
				}
			}
			serverSocket.close(); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}