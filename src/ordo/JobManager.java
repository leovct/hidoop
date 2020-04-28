package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

import formats.Format;
import map.MapReduce;

public interface JobManager extends Remote {

    /**
     * Add a job request to the list of jobs
     * 
     * @param mapperReducer
     * @param formatType
     * @param filename
     * @return int representing the id given to the job
     * @throws RemoteException
     */
    public int addJob(MapReduce mapperReducer, Format.Type formatType, String filename) throws RemoteException;

    /**
     * Start the job corresponding to the id
     * 
     * @param id
     * @throws RemoteException
     */
    public void startJob(int id) throws RemoteException;

    /**
     * Delete a job 
     * 
     * @param id
     * @throws RemoteException
     */
    public void deleteJob(int id) throws RemoteException;

    /**
	 * Notify the NameNode of the availability of a Daemon on a given server.
	 * 
	 * @param serverAddress address of the server running the Daemon
     * @throws RemoteException
	 */
	public void notifyDaemonAvailability(String serverAddress) throws RemoteException;

    /**
	 * Get the list of Daemons known currently available by the NameNode.
	 * 
	 * @return ArrayList<String> containing the names addresses of available daemons
	 * @throws RemoteException
	 */
	public ArrayList<String> getAvalaibleDaemons() throws RemoteException;

}

