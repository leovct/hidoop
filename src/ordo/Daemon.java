package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer,Callback cb) throws RemoteException;
}
