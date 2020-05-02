package ordo;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import formats.Format;
import map.MapReduce;

public class JobData implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 
     * Data of the job
     * fileName : name of the file 
     * fileType : type of the file 
     * state : State of the job : Done, Running, Suspended or NotStarted
     */
    private String fileName;
    private Format.Type fileType;
    private State jobState;
    private MapReduce mapperReducer;
    private int nbMapsDone;
    
	/**
	 * Map states of the job.
	 * Keys (Integer) : ID of the chunks
	 * Value (Boolean) : True if the map is done, else False
	 */
    private ConcurrentHashMap<Integer,Boolean> mapState;
    
	/**
	 * Constructor.
	 * 
	 * @param fileName 
	 * @param fileType
	 * @param mapperReducer
	 */
	public JobData(String fileName, Format.Type fileType, MapReduce mapperReducer) {
		this.fileName = fileName;
		this.fileType = fileType;
        this.jobState = State.NotStarted;
        this.mapperReducer = mapperReducer;
        this.nbMapsDone = 0;
		this.mapState = new ConcurrentHashMap<Integer,Boolean>();
    }

    /**
     * Add a map
     * @param chunk_id number of the chunk
     */
    public void addMapState(int chunk_id) {
        this.mapState.put(chunk_id, false);
    }

    /**
     * Modify the state of a map
     * @param chunk_id number of the chunk
     * @param state boolean representing the state of the map
     */
    public void setMapState(int chunk_id, boolean state) {
        this.mapState.replace(chunk_id, state);
        if (state == true) {
            this.nbMapsDone++;
        } else {
            this.nbMapsDone--;
        }
    }

    /**
     * Récupère un map dans la hashmap de l'état des maps
     * @param chunk_id number of the chunk
     * @return boolean representing the state of the map
     */
    public boolean getMapState(int chunk_id) {
        return this.mapState.get(chunk_id);
    }

    /**
     * Getters et Setters
     */

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Format.Type getFileType() {
        return fileType;
    }

    public void setFileType(Format.Type fileType) {
        this.fileType = fileType;
    }

    public State getJobState() {
        return jobState;
    }

    public void setJobState(State jobState) {
        this.jobState = jobState;
    }

    public ConcurrentHashMap<Integer, Boolean> getMapState() {
        return mapState;
    }

    public void setMapState(ConcurrentHashMap<Integer, Boolean> mapState) {
        this.mapState = mapState;
    }

    public int getNbMapsDone() {
        return nbMapsDone;
    }

    public void setNbMapsDone(int nbMapsDone) {
        this.nbMapsDone = nbMapsDone;
    }

    public MapReduce getMapperReducer() {
        return mapperReducer;
    }

    public void setMapperReducer(MapReduce mapperReducer) {
        this.mapperReducer = mapperReducer;
    }     
    
}