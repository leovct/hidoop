package ordo;

import java.rmi.*;
import java.rmi.registry.*;
import java.util.*;
import java.lang.*;
import map.*;
import formats.*;
import application.*;
import java.io.*;
import hdfs.*;
import config.*;

public class Job implements JobInterface {

    
    private Format.Type inputFormat;
    private Format.Type outputFormat;
    private Format.Type resReduceFormat;
    private String inputFName;
    private String outputFName;
    private String resReduceFName;
    private int nbMaps; 
    private HashMap<String,String> chunkList = new HashMap<String,String>();
  
    public Job(Format.Type inputFormat, String inputFName) {
    	this.inputFormat = inputFormat;
    	this.inputFName = inputFName;
    	this.outputFormat = Format.Type.KV;
    	this.outputFName = (inputFName.split("\\.")[0] + "-map" + "." + inputFName.split("\\.")[1]);
    	this.resReduceFormat = Format.Type.KV;
    	this.resReduceFName = (inputFName.split("\\.")[0] + "-resf" + "." + inputFName.split("\\.")[1]);
    }

    public void startJob (MapReduce mr){
    	
	System.out.println("Lancement de startJob ...");
                        
        // Création des formats
        Format input = new LineFormat(getInputFName());
        Format output = new KVFormat(getOutputFName());
	Format resReduce = new KVFormat(getResReduceFName());
    	    
    	// Récupération de la liste des démons
    	System.out.println("Récupération de la liste des Daemons ...");
        List<Daemon> demons = new ArrayList<>();
        for(int i = 0; i < Project.NBR_DATANODE; i++) {
        	try {
        		System.out.println("On récupère le stub de : //"+Project.HOST[i]+":4321/DaemonImpl"+ (i+1) );
    			demons.add((Daemon) Naming.lookup("//"+Project.HOST[i]+":4321/DaemonImpl"+ (i+1) ));
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
        }
	System.out.println("Daemons récupérés !!\n");

	System.out.println("Récupération du NameNode ...");
	
    	HashMap<String,String> mapList = new HashMap<String,String>();
    	NameNode nm = null;
	try {
        // Récupération du NameNode et de la liste des chunks
		System.out.println("On récupère le stub de : //bilbo:"+Project.PORT_NAMENODE+"/NameNode");
		nm = (NameNode)Naming.lookup("//"+Project.NAMENODEHOST+":"+Project.PORT_NAMENODE+"/NameNode");
		System.out.println("NameNode récupéré !!\n");
		RFiche cl = nm.Consulter(getInputFName());
		setChunkList(cl.getNode());
        // Ajout temporaire dans le NameNode des fichiers résultats des maps
        for (String nomKey : getChunkList().keySet()) {
           	mapList.put(getInputFName().split("\\.")[0]+(nomKey.split("\\.")[0]).split(getInputFName().split("\\.")[0])[1]+"-map.txt",getChunkList().get(nomKey)); 
        }
		CFicheImpl l = new CFicheImpl(getInputFName().split("\\.")[0]+"-map.txt",mapList);
            	nm.Ajouter(l);
    	} catch (Exception e) {
		e.printStackTrace();
	} 

	// Mise à jour du nombre de maps à effectuer
	setNbMaps(getChunkList().size());
	
        // Initialisation du callback
        System.out.println("Initialisation du Callback ...");
        Callback cb = null;
        try {
        	cb = new CallbackImpl(getNbMaps());
        } catch (RemoteException e) {
        	e.printStackTrace();
        }
	System.out.println("CallBack initialisé !!\n");
        	
        // Lancement des maps sur les démons
        System.out.println("Lancement des maps ...");
        	for(int i = 0; i < getNbMaps(); i++) {
			// On définit le nom du chunk
			String chunk = getInputFName().split("\\.")[0] + i + "." + getInputFName().split("\\.")[1];
			// On récupère le nom de la machine qui possède le chunk
    			String machine = getChunkList().get(chunk);
			// On récupère le numéro du démon sur lequel lancer le map
    			int numDaemon = Arrays.asList(Project.HOST).indexOf(machine);
			// On récupère le bon démon dans la liste des démons
			    Daemon d = demons.get(numDaemon);
			// On change le nom des Formats pour qu'ils correspondent aux fragments
    			Format inputTmp = new LineFormat(chunk);
    	        	Format outputTmp = new KVFormat(chunk.split("\\.")[0] + "-map." + chunk.split("\\.")[1]);
    			// On appelle runMap sur le bon démon
			try {
				d.runMap(mr, inputTmp, outputTmp, cb);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
       		}
        	System.out.println("Maps terminés !!\n");
        	
    		// Puis on attends que tous les démons aient finis leur travail
        	System.out.println("Attente du callback des Daemons ...");
        	try {
        		cb.waitMapDone();
        	} catch (RemoteException e) {
        		e.printStackTrace();
        	}
		System.out.println("Callback reçu !!\n");
			
	// On appelle hdfs.read pour récupérer tous les résultats des maps
        try {
        	HdfsClient.HdfsRead(getInputFName().split("\\.")[0] +"-map.txt" ,getOutputFName());
		nm.Supprimer(getInputFName().split("\\.")[0] +"-map.txt");
        } catch (Exception e) {
                e.printStackTrace();
        } 

	// On peut alors lancer le reduce
	System.out.println("Ouverture du fichier contenant la concaténation des résultats des maps");
        output.open(Format.OpenMode.R);
        System.out.println("Ouverture du fichier dans lequel les résultats du reduce doivent être écrits ");
        resReduce.open(Format.OpenMode.W);
        System.out.println("Lancement du reduce ...");
	mr.reduce(output, resReduce);
	System.out.println("Reduce terminé !!!");
	System.out.println("Fermeture des fichiers en lecture et écriture ...");
	output.close();
	resReduce.close();
	System.out.println("Job terminé !!");
    }
    

    public void setInputFormat(Format.Type format){
        this.inputFormat = format;
    }

    public Format.Type getInputFormat() {
    	return this.inputFormat;
    }
    
    public void setInputFname(String fname){
        this.inputFName = fname;
    }
    
    public String getInputFName() {
    	return this.inputFName;
    }
    
    public void setOutputFormat(Format.Type format){
        this.outputFormat = format;
    }
    
    public Format.Type getOutputFormat() {
    	return this.outputFormat;
    }

    public void setOutputFname(String fname){
        this.outputFName = fname;
    }
    
    public String getOutputFName() {
    	return this.outputFName;
    }
    
    public void setResReduceFormat(Format.Type format){
        this.resReduceFormat = format;
    }
    
    public Format.Type getResReduceFormat() {
    	return this.resReduceFormat;
    }

    public void setResReduceFname(String fname){
        this.resReduceFName = fname;
    }
    
    public String getResReduceFName() {
    	return this.resReduceFName;
	}
    

    public void setNbMaps(int maps){
        this.nbMaps = maps;
    }

    public int getNbMaps() {
    	return this.nbMaps;
    }

    public void setChunkList(HashMap<String,String> chunkList) {
	this.chunkList = chunkList;
    }

    public HashMap<String,String> getChunkList() {
	return this.chunkList;
    }
}

