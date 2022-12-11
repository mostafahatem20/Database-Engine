package Notsosoftware;

import java.awt.Dimension;

import java.awt.Polygon;
import java.io.IOException;
import java.text.ParseException;

public class PolygonDB extends Polygon implements Comparable {
	public Polygon p;

	public PolygonDB() {
		this.p = new Polygon();
	}

	public PolygonDB(Polygon p) {
		this.p = p;
	}

	public double getArea() {
		Dimension dim1;

		dim1 = this.p.getBounds().getSize();

		double nThisArea1;

		nThisArea1 = dim1.width * dim1.height;
//		System.out.println(dim1.width);
//		System.out.println(dim1.height);
		return nThisArea1;
	}

	public PolygonDB(int[] a, int[] b, int i) {
		this.p = new Polygon(a, b, i);
	}

	@Override

	public int compareTo(Object o) {

		Dimension dim1;

		dim1 = this.p.getBounds().getSize();

		double nThisArea1;

		nThisArea1 = dim1.width * dim1.height;

		Dimension dim2;

		dim2 = ((Polygon) o).getBounds().getSize();

		double nThisArea2;

		nThisArea2 = dim2.width * dim2.height;

		if (nThisArea1 > nThisArea2)

			return 1;

		else if (nThisArea1 < nThisArea2)

			return -1;

		return 0;

	}

	public boolean equals(Object o) {
		Polygon p1 = (Polygon) o;
		int p1j = -1;
		int p2j = -1;
		boolean flag=true;

		if(this.p.xpoints.length != p1.xpoints.length)
		return false;
		
		for (int j = 0; j < p1.xpoints.length; j++) {
			if (p.xpoints[0] == p1.xpoints[j]) {
				if (p.ypoints[0] == p1.ypoints[j]) {
					p1j = j;
					p2j = j;
					break;
				}
			}

		}
		if (p1j != -1) {
			for (int i = 1; i < this.p.xpoints.length; i++) {
				if (p1j + 1 == p1.xpoints.length) {
					p1j = 0;
				} else {
					p1j++;
				}
				if (p.xpoints[i] == p1.xpoints[p1j]) {
					if (p.ypoints[i] != p1.ypoints[p1j]) {
						flag= false;
					}
				} else {
					flag= false;
				}
			}
		} else {
			return false;
		}
		if(!flag) {
			flag =true;
			
			for (int i = this.p.xpoints.length -1; i>0; i--) {
				if (p2j + 1 == p1.xpoints.length) {
					p2j = 0;
				} else {
					p2j++;
				}
				if (p.xpoints[i] == p1.xpoints[p2j]) {
					if (p.ypoints[i] != p1.ypoints[p2j]) {
						flag= false;
					}
				} else {
					flag= false;
				}
			}
		}
		
		return flag;
	}

}