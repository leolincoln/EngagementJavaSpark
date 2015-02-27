import java.io.Serializable;

public class HcResults implements Serializable {
	Integer subject;
	String id1, id2;
	Double corr;

	public HcResults(Integer subject, String id1, String id2, Double corr) {
		this.subject = subject;
		this.id1 = id1;
		this.id2 = id2;
		this.corr = corr;
	}

	public void setSubject(Integer subject) {
		this.subject = subject;
	}

	public void setId1(String id1) {
		this.id1 = id1;
	}

	public void setId2(String id2) {
		this.id2 = id2;
	}

	public void setCor(Double corr) {
		this.corr = corr;
	}

	public Integer getSubject() {
		return this.subject;
	}

	public String getId1() {
		return this.id1;
	}

	public String getId2() {
		return this.id2;
	}

	public Double getCorr() {
		return this.corr;
	}

}
