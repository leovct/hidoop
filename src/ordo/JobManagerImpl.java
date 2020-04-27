package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import formats.Format.Type;
import map.MapReduce;

public class JobManagerImpl extends UnicastRemoteObject implements JobManager {

    protected JobManagerImpl() throws RemoteException {
        super();
        // TODO Auto-generated constructor stub
    }

    private static final long serialVersionUID = 1L;

    @Override
    public int addJob(MapReduce mapperReducer, Type formatType, String filename) throws RemoteException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void startJob(int id) throws RemoteException {
        // TODO Auto-generated method stub

    }

    @Override
    public void printJobs() throws RemoteException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteJob(int id) throws RemoteException {
        // TODO Auto-generated method stub

    }

    @Override
    public void notifyDaemonAvailability(String serverAddress) throws RemoteException {
        // TODO Auto-generated method stub

    }

    @Override
    public ArrayList<String> getAvalaibleDaemons() throws RemoteException {
        // TODO Auto-generated method stub
        return null;
    }


}

