import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test {
	private Integer x, y, z;
	private List<Integer> data;

	public Test(Integer x, Integer y, Integer z, List<Integer> data) {
		// this.subject = subject;
		this.x = x;
		this.y = y;
		this.z = z;
		this.data = data;
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

	public void setData(List<Integer> data) {
		this.data = data;
	}

	public List<Integer> getData() {
		return this.data;
	}

	public String toString() {
		return x + "|" + y + "|" + z + "|" + Arrays.toString(data.toArray());
	}
}
