package hdfs;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import config.Project;

public class NameNodeImpl extends UnicastRemoteObject implements NameNode {
	private static final long serialVersionUID = 1L;

	protected NameNodeImpl() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public Integer[] writeChunkRequest() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer[] readFileRequest(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	public static void main(String[] args) {
		try{
			LocateRegistry.createRegistry(Project.PORT_NAMENODE);
		}catch(Exception ex){
			System.out.println("Registre déjà créé");
		}
		try {
			NameNode nm = new NameNodeImpl();
			Naming.rebind("//"+Project.NAMENODEHOST+":"+Project.PORT_NAMENODE+"/NameNode", nm);
			System.out.println("NameNode bound in registry");
		} catch (Exception e) {
			e.printStackTrace() ;
		}
	}
}
