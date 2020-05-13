package hdfs;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A FileData object stores metadata of a file written on HDFS.
 */
public class FileData implements Serializable {
	private static final long serialVersionUID = 1L;
	
	/**
	 * Data of the file.
	 * fileSize : Number of chunks composing the file
	 * chunkSize : Size of each chunk
	 * replicationFactor : replication factor of chunks
	 */
	private int fileSize, chunkSize, replicationFactor;
	
	/**
	 * Chunk handles of the file.
	 * Keys (Integer) : ID of the chunks
	 * ArrayList<String> : Servers where copies are stored
	 */
	private ConcurrentHashMap<Integer,CopyOnWriteArrayList<String>> chunkHandles;

	/**
	 * Constructor.
	 * 
	 * @param fileSize number of chunks composing the file
	 * @param chunkSize size of chunks
	 * @param replicationFactor replication factor
	 */
	public FileData(int fileSize, int chunkSize, int replicationFactor) {
		this.fileSize = fileSize;
		this.chunkSize = chunkSize;
		this.replicationFactor = replicationFactor;
		this.chunkHandles = new ConcurrentHashMap<Integer,CopyOnWriteArrayList<String>>();
	}

	/**
	 * Add a copy location for specified chunk.
	 * 
	 * @param chunkId
	 * @param server
	 */
	public void addChunkLocation(int chunkId, String server) {
		if (this.chunkHandles.containsKey(chunkId)) {
			this.chunkHandles.get(chunkId).add(server);
		} else {
			CopyOnWriteArrayList<String> value = new CopyOnWriteArrayList<String>();
			value.add(server);
			this.chunkHandles.put(chunkId, value);
		}
	}
	
	/**
	 * Indicates if all chunk handles are available for this file.
	 * 
	 * @return boolean, true if all chunk handles are known
	 */
	public boolean fileComplete() {
		if (this.fileSize == -1) return false;
		for (int chunkHandle = 0 ; chunkHandle < this.fileSize ; chunkHandle++) {
			if (!this.containsChunkHandle(chunkHandle)) return false;
		}
		return true;
	}
	
	/**
	 * Get locations of all copies of specified chunk.
	 * 
	 * @param chunkId ID of the chunk
	 * @return
	 */
	public CopyOnWriteArrayList<String> getChunkHandle(int chunkId) {
		return this.chunkHandles.get(chunkId);
	}

	/**
	 * Tells if the specified chunk handle is stored in the FileData.
	 * 
	 * @param chunkId ID of the chunk
	 * @return
	 */
	public boolean containsChunkHandle(int chunkId) {
		return this.chunkHandles.containsKey(chunkId);
	}

	/**
	 * Getters and Setters
	 */
	public int getFileSize() {
		return fileSize;
	}

	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public ConcurrentHashMap<Integer, CopyOnWriteArrayList<String>> getChunkHandles() {
		return chunkHandles;
	}
}
