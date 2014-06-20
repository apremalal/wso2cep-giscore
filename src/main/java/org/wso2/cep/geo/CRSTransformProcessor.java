package org.wso2.cep.geo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.factory.epsg.FactoryUsingWKT;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import com.vividsolutions.jts.geom.Coordinate;

@SiddhiExtension(namespace = "geo", function = "crstransform")
public class CRSTransformProcessor extends TransformProcessor {

	private Map<String, Integer> paramPositions = new HashMap<String, Integer>();
	private String sourcecrs, targetcrs;
	private String latAttrName, longAttrName;

	public CRSTransformProcessor() {
		this.outStreamDefinition =
		                           new StreamDefinition().name("geoStream")
		                                                 .attribute("lattitude",
		                                                            Attribute.Type.DOUBLE)
		                                                 .attribute("longitude",
		                                                            Attribute.Type.DOUBLE);
	}

	@Override
	protected InStream processEvent(InEvent inEvent) {

		double sourceLat = (Double) inEvent.getData(paramPositions.get(latAttrName));
		double sourceLon = (Double) inEvent.getData(paramPositions.get(longAttrName));

		CoordinateReferenceSystem sourceCrs = null;
		CoordinateReferenceSystem targetCrs = null;

		try {
			sourceCrs = CRS.decode(sourcecrs);
			targetCrs = CRS.decode(targetcrs);
		} catch (NoSuchAuthorityCodeException e) {
			e.printStackTrace();
		} catch (FactoryException e) {
			e.printStackTrace();
		}

		Coordinate sourceCoordinate = new Coordinate(sourceLat, sourceLon);
		Coordinate targetCoordinate = new Coordinate();

		boolean lenient = true;
		MathTransform mathTransform = null;

		try {
			mathTransform = CRS.findMathTransform(sourceCrs, targetCrs, lenient);
		} catch (FactoryException e) {
			e.printStackTrace();
		}

		try {
			JTS.transform(sourceCoordinate, targetCoordinate, mathTransform);
		} catch (TransformException e1) {
			e1.printStackTrace();
		}

		double targetLat = targetCoordinate.x;
		double targetLon = targetCoordinate.y;
		Object[] data = new Object[] { targetLat, targetLon };

		return new InEvent(inEvent.getStreamId(), System.currentTimeMillis(), data);
	}

	@Override
	protected InStream processEvent(InListEvent inListEvent) {
		InListEvent transformedListEvent = new InListEvent();
		for (Event event : inListEvent.getEvents()) {
			if (event instanceof InEvent) {
				transformedListEvent.addEvent((Event) processEvent((InEvent) event));
			}
		}
		return transformedListEvent;
	}

	@Override
	protected Object[] currentState() {
		return new Object[] { paramPositions };
	}

	@Override
	protected void restoreState(Object[] objects) {
		if (objects.length > 0 && objects[0] instanceof Map) {
			paramPositions = (Map<String, Integer>) objects[0];
		}
	}

	@Override
	protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors,
	                    StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
	                    String elementId, SiddhiContext siddhiContext) {

		for (Expression parameter : parameters) {
			if (parameter instanceof Variable) {
				Variable var = (Variable) parameter;
				String attributeName = var.getAttributeName();
				paramPositions.put(attributeName,
				                   inStreamDefinition.getAttributePosition(attributeName));
			}
		}
		sourcecrs = ((StringConstant) parameters[0]).getValue();
		targetcrs = ((StringConstant) parameters[1]).getValue();
		latAttrName = ((Variable) parameters[2]).getAttributeName();
		longAttrName = ((Variable) parameters[3]).getAttributeName();

	}

	public void destroy() {
	}
}