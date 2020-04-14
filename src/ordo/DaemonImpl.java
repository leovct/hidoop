package ordo;

import map.Mapper;
import formats.Format;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject ;
import java.rmi.registry.* ;
import java.util.*;
import java.util.regex.Pattern;
import java.net.InetAddress;
import java.lang.*;
import config.*;


public class DaemonImpl extends UnicastRemoteObject implements Daemon {

    private static final long serialVersionUID = 1L;

    public DaemonImpl() throws RemoteException {
    }
    
    public void runMap (Mapper m, Format reader, Format writer,Callback cb) throws RemoteException {
	// On créé un thread pour le map
	MapRunner mapRunner = new MapRunner(m, reader, writer, cb);
    	mapRunner.start();       
      
    }
    public static void main(String[] args) {
        int num;
        InetAddress adresse;
        try {
            // définition du port et du numéro du démon
            num = Integer.parseInt(args[0]);
        } catch (Exception ex) {
            System.out.println(" Usage : java ordo/DaemonImpl <numero du démon>"); 
            return;
        }
        
        try {
        	// Création du registre RMI
            System.out.println("Création du RMI ...");
            Registry registry = LocateRegistry.createRegistry(Project.PORT_DAEMON);
            System.out.println("RMI créé !");
        } catch (Exception e) {
            System.out.println("Le RMI existe déjà !");
        }
        
        try {
            // Enregistrement du démon dans le registre
	    adresse = InetAddress.getLocalHost();
	    	System.out.println(InetAddress.getLocalHost().getHostAddress());
	    	System.out.println(InetAddress.getByName("ohm.enseeiht.fr").getHostAddress());
	    	System.out.println("Is reachable : "+InetAddress.getByName("ohm.enseeiht.fr").isReachable(1000)); // Fonctionne quand le VPN est ON
	    	// Naming.rebind("//"+InetAddress.getLocalHost().getHostAddress()+":"+Project.PORT_DAEMON+"/DaemonImpl"+num,demon);
	    	// + ITERER SUR LES PORTS POSSIBLES
            System.out.println("Enregistrement du démon dans le registre");
            DaemonImpl demon = new DaemonImpl();
            Naming.rebind("//"+adresse.getHostName().split("\\.",2)[0]+":"+Project.PORT_DAEMON+"/DaemonImpl"+num,demon);
            System.out.println("//"+adresse.getHostName().split("\\.",2)[0]+":"+Project.PORT_DAEMON+"/DaemonImpl"+num+" bound in registry");
        } catch (Exception e) {
            System.out.println("Le port sur lequel vous souhaitez vous connecter est occupé !");
        }
    }

}
