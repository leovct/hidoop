package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

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
	 * Chunk handles fo the file.
	 * Keys (Integer) : ID of the chunks
	 * ArrayList<String> : Servers where copies are stored
	 */
	private ConcurrentHashMap<Integer,ArrayList<String>> chunkHandles;

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
		this.chunkHandles = new ConcurrentHashMap<Integer,ArrayList<String>>();
	}

	/**
	 * Add a copy location for specified chunk.
	 * 
	 * @param chunkId
	 * @param server
	 */
	public void addChunkLocation(int chunkId, String server) {
		if (this.chunkHandles.contains(chunkId)) {
			this.chunkHandles.get(chunkId).add(server);
		} else {
			ArrayList<String> value = new ArrayList<String>();
			value.add(server);
			this.chunkHandles.put(chunkId, value);
		}
	}
	
	/**
	 * Indicates if all chunk handles are avalaible for this file.
	 * 
	 * @return boolean, true if all chunk hundles are known
	 */
	public boolean fileComplete() {
		if (this.fileSize == -1) return false;
		for (int chunkHundle = 0 ; chunkHundle < this.fileSize ; chunkHundle++) {
			if (!this.containsChunkHandle(chunkHundle)) return false;
		}
		return true;
	}
	
	
	/**
	 * Get locations of all copies of specified chunk.
	 * 
	 * @param chunkId ID of the chunk
	 * @return
	 */
	public ArrayList<String> getChunkHundle(int chunkId) {
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

	public ConcurrentHashMap<Integer, ArrayList<String>> getChunkHandles() {
		return chunkHandles;
	}
}
