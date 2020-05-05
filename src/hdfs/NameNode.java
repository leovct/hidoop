package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * NameNode Interface.
 */
public interface NameNode extends Remote {
	/**
	 * Send a request to write a chunk on HDFS
	 * with NameNode's default replication factor.
	 *
	 * @return ArrayList<String> containing :
	 * {primary DataNode address, secondary DataNode address (first replica location),...}
	 */
	public ArrayList<String> writeChunkRequest() throws RemoteException;
	
	/**
	 * Send a request to write a chunk on HDFS
	 * with specified replication factor.
	 * 
	 * @param replicationFactor
	 * @return 
	 * @return ArrayList<String> containing :
	 * {primary DataNode address, secondary DataNode address (first replica location),...}
	 */
	public ArrayList<String> writeChunkRequest(int replicationFactor) throws RemoteException;
	
	/**
	 * Send a request to read a file on HDFS.
	 * 
	 * @param fileName
	 * @return ArrayList<String> containing the addresses of servers containing the chunks
	 * (for each chunk, only one of the replicas' locations is given)
	 */
	public ArrayList<ArrayList<String>> readFileRequest(String fileName) throws RemoteException;
	
	/**
	 * Send a request to delete a file on HDFS.
	 * 
	 * @param fileName
	 * @return ArrayList<String> containing the addresses of servers containing the chunks
	 * (for each chunk, only one of the replicas' locations is given)
	 */
	public ArrayList<String> deleteFileRequest(String fileName) throws RemoteException;
	
	/**
	 * Notify the NameNode that a chunk has been written on a server.
	 * 
	 * @param fileName name of the file the chunk is part of
	 * @param fileSize size of the file 
	 * @param chunkSize size of the chunk
	 * @param replicationFactor replication factor of the chunk
	 * @param chunkNumber number of the chunk in the file
	 * @param server server containing the chunk
	 * @throws RemoteException
	 */
	public void chunkWritten(String fileName, int fileSize, int chunkSize, int replicationFactor, int chunkNumber, String server) throws RemoteException;
	
	/**
	 * Notify the NameNode that all chunk have been
	 * stored on servers for specified file.
	 * 
	 * @param fileName specified file
	 */
	public void allChunkWritten(String fileName) throws RemoteException;
	
	/**
	 * Notify the NameNode that a chunk has been deleted on a server.
	 * 
	 * @param fileName name of the file the chunk is part of
	 * @param chunkNumber number of the chunk in the file
	 * @param server server that contained the chunk
	 */
	public void chunkDeleted(String fileName, int chunkNumber, String server) throws RemoteException;
	
	/**
	 * Notify the NameNode of the availability of a DataNode on a given server.
	 * 
	 * @param serverAddress address of the server running the DataNode
	 */
	public void notifyNameNodeAvailability(String serverAddress) throws RemoteException;
}
