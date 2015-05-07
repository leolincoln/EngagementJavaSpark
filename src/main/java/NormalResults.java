import java.io.Serializable;

public class NormalResults implements Serializable {
	String id1, id2;
	Boolean normal;

	public NormalResults(String id1, String id2, Boolean normal) {
		this.id1 = id1;
		this.id2 = id2;
		this.normal = normal;
	}

	public String getId1() {
		return this.id1;
	}

	public String getId2() {
		return this.id2;
	}

	public Boolean getNormal() {
		return this.normal;
	}

	public void setId1(String id1) {
		this.id1 = id1;
	}

	public void setId2(String id2) {
		this.id2 = id2;
	}

	public void setNormal(Boolean normal) {
		this.normal = normal;
	}
}
