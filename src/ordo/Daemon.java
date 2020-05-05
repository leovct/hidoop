package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;
import map.Reducer;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer, long jobId) throws RemoteException;

	public void runReduce(Reducer r, Format reader, Format writer, long jobId) throws RemoteException;
}
