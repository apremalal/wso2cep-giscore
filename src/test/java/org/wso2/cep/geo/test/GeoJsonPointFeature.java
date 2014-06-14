package org.wso2.cep.geo.test;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class GeoJsonPointFeature {
	private String type = "Feature";
	private GeoJsonPoint geometry;
	private Map<String, Object> properties = new HashMap<String, Object>();

	public GeoJsonPointFeature() {

	}

	public GeoJsonPoint getGeometry() {
		return geometry;
	}

	public void setGeometry(GeoJsonPoint geometry) {
		this.geometry = geometry;
	}

	public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}

	public GeoJsonPointFeature(String deviceId, String timeStamp) {
		properties.put("deviceid", deviceId);
		properties.put("timestamp", timeStamp);
	}

	public void setProperty(String key, String value) {
		properties.put(key, value);
	}

	public String getAsGeoJson() {
		return new Gson().toJson(this);
	}
}
