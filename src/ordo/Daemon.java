package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import map.Mapper;
import formats.Format;
import config.*;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer,Callback cb) throws RemoteException;
}
