package ordo;
import map.*;
import formats.*;

public interface JobInterface {
	
    public void setNbMaps(int tasks);
    public void setOutputFormat(Format.Type ft);
    public void setOutputFname(String fname);
    
    public int getNbMaps();
    public Format.Type getInputFormat();
    public Format.Type getOutputFormat();
    public String getInputFname();
    public String getOutputFname();
	
    public void startJob (MapReduce mr);
}
