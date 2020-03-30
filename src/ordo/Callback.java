package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Callback extends Remote {
	public void incNbMapDone () throws RemoteException;
	public void waitMapDone() throws RemoteException;
}