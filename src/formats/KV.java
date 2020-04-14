package formats;

public class KV {

	public static final String SEPARATOR = "<->";
	
	public String k;
	public String v;
	
	public KV() {}
	
	public KV(String k, String v) {
		super();
		this.k = k;
		this.v = v;
	}

	public byte[] writeSyntax(Format.Type type) {
		if (type == Format.Type.LINE) {
			return (this.v+"\n").getBytes();
		} else if (type == Format.Type.KV) {
			return (this.k+KV.SEPARATOR+this.v+"\n").getBytes();
		} else return null;
	}
	
	public String toString() {
		return "KV [k=" + k + ", v=" + v + "]";
	}
	
}
