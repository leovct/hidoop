package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;
import map.Reducer;

public interface Daemon extends Remote {

	/**
	 * Manage to launch a map task
	 * @param m
	 * @param reader
	 * @param writer
	 * @param jobId
	 * @throws RemoteException
	 */
	public void runMap (Mapper m, Format reader, Format writer, long jobId) throws RemoteException;

}
