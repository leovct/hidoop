package ordo;
import map.*;
import formats.*;

public interface JobInterface {
//Méthodes requises pour la classe Job  
	public void setInputFormat(Format.Type format);
	public Format.Type getInputFormat();
    public void setInputFname(String fname);
    public String getInputFName();
    public void setOutputFormat(Format.Type format);
    public Format.Type getOutputFormat();
	public void setOutputFname(String Fname);
	public String getOutputFName();
	public void setResReduceFormat(Format.Type format);
	public Format.Type getResReduceFormat();
	public void setResReduceFname(String Fname);
	public String getResReduceFName();
	
    public void startJob (MapReduce mr);
}
