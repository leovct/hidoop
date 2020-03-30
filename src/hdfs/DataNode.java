package hdfs;

import java.rmi.Remote;
	
/**
 * Implémentation du DataNode d'un serveur
 * Communication avec le NameNode par sockets, communication
 * avec les démons locaux via RMI
 *
 */
public interface DataNode extends Remote {
	/**
	 * Notifier le NameNode de l'ajout d'un chunk au serveur
	 * 
	 * @param fileName le nom du fichier d'où est issu le chunk
	 * @param chunkName le nom du chunk sur le serveur
	 * @return un booleen, true en cas de réussite
	 */
	public boolean notifyNameNode(String fileName, String chunkName);
}