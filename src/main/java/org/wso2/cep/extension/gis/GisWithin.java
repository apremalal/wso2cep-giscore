package org.wso2.cep.extension.gis;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.measure.Latitude;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

@SiddhiExtension(namespace = "geo", function = "iswithin")
public class GisWithin extends FunctionExecutor {

	Logger log = Logger.getLogger(GisWithin.class);
	Attribute.Type returnType;

	/**
	 * Method will be called when initialising the custom function
	 * 
	 * @param types
	 * @param siddhiContext
	 */
	
	@Override
	public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {
		returnType = Attribute.Type.BOOL;
	}

	/**
	 * Method called when sending events to process
	 * 
	 * @param obj
	 * @return
	 */
	@Override
	protected Object process(Object obj) {

		Object functionParams[] = (Object[]) obj;
		String strPoint = (String) functionParams[0];
		String strPolygon = (String) functionParams[1];

		JsonElement jsonElement = new JsonParser().parse(strPoint);
		JsonObject jObject = jsonElement.getAsJsonObject();

		JsonArray jLocCoordinatesArray = jObject.getAsJsonArray("coordinates");

		double lattitude = Double.parseDouble(jLocCoordinatesArray.get(0)
				.toString());
		double longitude = Double.parseDouble(jLocCoordinatesArray.get(1)
				.toString());

		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

		/* Creating a point */
		Coordinate coord = new Coordinate(lattitude, longitude);
		Point point = geometryFactory.createPoint(coord);
		

		jsonElement = new JsonParser().parse(strPolygon);
		jObject = jsonElement.getAsJsonObject();

		// consdering without holes scenario
		jLocCoordinatesArray = (JsonArray) jObject
				.getAsJsonArray("coordinates").get(0);
		Coordinate[] coords = new Coordinate[jLocCoordinatesArray.size()];
		for (int i = 0; i < jLocCoordinatesArray.size(); i++) {
			JsonArray jArray = (JsonArray) jLocCoordinatesArray.get(i);
			coords[i] = new Coordinate(Double.parseDouble(jArray.get(0)
					.toString()), Double.parseDouble(jArray.get(1).toString()));
		}

		LinearRing ring = geometryFactory.createLinearRing(coords);
		LinearRing holes[] = null; // use LinearRing[] to represent holes
		Polygon polygon = geometryFactory.createPolygon(ring, holes);

		return point.within(polygon);
	}

	@Override
	public void destroy() {

	}

	/**
	 * Return type of the custom function mentioned
	 * 
	 * @return
	 */
	@Override
	public Attribute.Type getReturnType() {
		return returnType;
	}

}
