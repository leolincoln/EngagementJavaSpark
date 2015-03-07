package old_codes;
import java.io.Serializable;

public class Hitchcockdatatotal implements Serializable {
	/**
	 * generated serialization id
	 */
	private static final long serialVersionUID = 4614441050244688113L;
	private Integer subject, time, x, y, z, data;
	private String id;

	public Hitchcockdatatotal(Integer x, Integer y, Integer z,
			Integer subject, Integer time, Integer data) {
		this.subject = subject;
		this.time = time;
		this.x = x;
		this.y = y;
		this.z = z;
		this.data = data;
		this.id = x + "|" + y + "|" + z + "|" + subject;
	}

	public void setSubject(Integer subject) {
		this.subject = subject;
	}

	public String getID() {
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