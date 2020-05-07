package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

import config.SettingsManager.Command;
	
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
	 * @param chunkNumber 
	 * @return port number to send data on
	 */
	public int processChunk(Command command, String fileName, int chunkNumber) throws RemoteException;
}