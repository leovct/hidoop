package hdfs;

import java.rmi.Remote;

public interface NameNode extends Remote {
	/**
	 * Envoyer une requête d'écriture d'un chunk sur HDFS
	 * 
	 * Paramètres : Aucun, on veut juste savoir ou on peut écrire un chunk,
	 * les données seront transmises au nameNode par le dataNode
	 * @return tableau d'entier qui contient : [n° de version, namenode primaire, namenode 2,...]
	 */
	public Integer[] writeChunkRequest();
	
	
	/**
	 * Envoyer une requête de lecture d'un fichier sur HDFS
	 * 
	 * @param fileName
	 * @return tableau d'entier qui contient les identifiants des serveurs où se trouvent les chunk
	 */
	public Integer[] readFileRequest(String fileName);
}
