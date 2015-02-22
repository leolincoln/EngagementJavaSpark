import java.io.Serializable;

public class Hitchcockdatatotal_new implements Serializable {
	/**
	 * generated serialization id
	 */
	private static final long serialVersionUID = 4614441050244688113L;
	private Integer subject, time, x, y, z, data;
	private Integer id;

	public Hitchcockdatatotal_new(Integer x, Integer y, Integer z, Integer subject,
			Integer time, Integer data) {
		this.subject = subject;
		this.time = time;
		this.x = x;
		this.y = y;
		this.z = z;
		this.data = data;
		this.id = subject * 100000000 + x * 1000000 + y * 10000 + z * 100;
	}

	public void setSubject(Integer subject) {
		this.subject = subject;
	}

	/**
	 * getID is the unique identifier of the string. Supposedly, for each
	 * hitchcock data, there is no more than 100 x y z subject then we space out
	 * each variable by 100 to the power of something.
	 * 
	 * @return the unique Integer ID for this row. 
	 */
	public Integer getID() {
		return this.id;
	}

	public Integer getSubject() {
		return this.subject;
	}

	public void setTime(Integer time) {
		this.time = time;
	}

	public Integer getTime() {
		return this.time;
	}

	public void setX(Integer x) {
		this.x = x;
	}

	public Integer getX() {
		return this.x;
	}

	public void setY(Integer y) {
		this.y = y;
	}

	public Integer getY() {
		return this.y;
	}

	public void setZ(Integer z) {
		this.z = z;
	}

	public Integer getZ() {
		return this.z;
	}

	public void setData(Integer data) {
		this.data = data;
	}

	public Integer getData() {
		return this.data;
	}

	@Override
	public String toString() {
		return subject + " " + time + " " + x + " " + y + " " + z + " " + data;
	}
}
