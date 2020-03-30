package ordo;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject ;
import java.util.*;
import java.lang.*;

public class CallbackImpl extends UnicastRemoteObject implements Callback {
	
	private static final long serialVersionUID = 2674880711467464646L;
	private int nbMapDone;  
	private int nbMaps;
	private final Object lock = new Object();

	public CallbackImpl(int maps) throws RemoteException {
		this.nbMapDone = 0;
		this.nbMaps = maps;
	}
	
	public void incNbMapDone () throws RemoteException {
		this.nbMapDone ++;
		synchronized(lock){
			lock.notify();
		}
		
	}
	
	public void waitMapDone() throws RemoteException {
		while (this.nbMapDone < nbMaps) {
			try {
				synchronized(lock){
					lock.wait();
				}
				
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
