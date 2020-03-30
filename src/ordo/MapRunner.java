package ordo;

import java.rmi.RemoteException;
import formats.Format;
import map.Mapper;

public class MapRunner extends Thread {
	
	Mapper m; //map à lancer
	Format reader, writer; //les formats de lecture et d'écriture
	Callback cb;
	
	public MapRunner(Mapper m, Format reader, Format writer, Callback cb){
		this.m = m;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}
	
	public void run() {
		System.out.println("Lancement de la tâche ... ");
		System.out.println("Ouverture du fichier contenant le fragment sur lequel exécuter le map");
        	reader.open(Format.OpenMode.R);
        	System.out.println("Ouverture du fichier dans lequel les résultats du map doivent être écrits ");
        	writer.open(Format.OpenMode.W);
        	System.out.println("Lancement du map sur le fragment ...");
		m.map(reader, writer);
		System.out.println("Map terminé !!!");
		System.out.println("Fermeture des fichiers en lecture et écriture ...");
		reader.close();
		writer.close();
		
		System.out.println("Envoie du callback ...");
		try {
        		cb.incNbMapDone();   
		} catch (RemoteException e) {  
			e.printStackTrace();
		}
        	System.out.println("Travail terminé !!!"); 
		
	}
}
