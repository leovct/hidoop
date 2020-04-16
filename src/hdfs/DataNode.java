package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

import config.Project.Command;
	
/**
 * Server NameNode interface
 *
 */
public interface DataNode extends Remote {
	
	/**
	 * Initiate opening of a TCP connection with the NameNode 
	 * to write a chunk on the server.
	 * @param command 
	 * @param fileName 
	 * @param fileExtension 
	 * @param chunkNumber 
	 * 
	 * @return port number to send data on
	 */
	public int processChunk(Command command, String fileName, String fileExtension, int chunkNumber) throws RemoteException;
}