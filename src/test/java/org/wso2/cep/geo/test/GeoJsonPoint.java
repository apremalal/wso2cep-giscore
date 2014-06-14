package org.wso2.cep.geo.test;

public class GeoJsonPoint {
	private String type = "Point";
	double coordinates[] = new double[2];

	public GeoJsonPoint(double x, double y) {
		coordinates[0] = x;
		coordinates[1] = y;
	}
	
	public String getType() {
		return type;
	}	

	public double[] getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(double[] coordinates) {
		this.coordinates = coordinates;
	}

}
