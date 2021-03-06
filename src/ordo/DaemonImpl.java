package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import config.SettingsManager;
import formats.Format;
import map.Mapper;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {
	private static final long serialVersionUID = 1L;
	private static String messageHeader = ">>> [DAEMON] ";
	private static String errorHeader = ">>> [ERROR] ";

	private String serverAddress;

	public DaemonImpl(String serverAddress) throws RemoteException {
		this.serverAddress = serverAddress;
	}


	public void runMap (Mapper m, Format reader, Format writer, long jobId) throws RemoteException {
		MapRunner mapRunner = new MapRunner(m, reader, writer, jobId, getServerAddress());
		mapRunner.start();    
	}

	/**
	 * Getter for serverAddress.
	 * @return
	 */
	public String getServerAddress() {
		return this.serverAddress;
	}
	
	/**
	 * Prints main usage on output stream.
	 */
	private static void printUsage() {
		System.out.println(errorHeader + "Incorrect parameters\nUsage :"
				+ "\njava DaemonImpl <server>\n With server = address of the server"
				+ " the DaemonImpl is executed on");
	}

	/**
	 * Main.
	 * Initializes a DaemonImpl instance and bounds it to the RMI registry
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length > 0) {
			try {
				//RMI registry creation
				LocateRegistry.createRegistry(SettingsManager.getPortDaemon());
				System.out.println(messageHeader+"RMI registry created");
			} catch (Exception e) {
				System.out.println(messageHeader+"RMI registry exists already");
			}

			try {
				//Bind the daemon to the RMI register
				DaemonImpl demon = new DaemonImpl(args[0]);
				Naming.rebind("//"+demon.getServerAddress()+":"+SettingsManager.getPortDaemon()+"/DaemonImpl",demon);
				System.out.println(messageHeader+"Daemon bound in registry");
				//Notify the JobManager of its availability
				JobManager jobManager = (JobManager) Naming.lookup("//"+SettingsManager.getMasterNodeAddress()+":"+SettingsManager.getPortJobMaster()+"/JobManager");
				jobManager.notifyDaemonAvailability(demon.getServerAddress());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else printUsage();
	} 
}
