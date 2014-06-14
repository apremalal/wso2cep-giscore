package org.wso2.cep.geo.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.wso2.cep.geo.GeoWithin;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

@RunWith(JUnit4.class)
public class GeoTestCase {

	private SiddhiManager siddhiManager;
	private String withinTrueQueryReference;
	private String withinFalseQueryReference;

	@Before
	public void setUpEnviornment() {
		SiddhiConfiguration conf = new SiddhiConfiguration();
		List<Class> classList = new ArrayList<Class>();
		classList.add(GeoWithin.class);
		conf.setSiddhiExtensions(classList);

		siddhiManager = new SiddhiManager();
		siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);
		siddhiManager
				.defineStream("define stream gpsInputStream (lattitude double, longitude double, deviceid string) ");

		withinTrueQueryReference = siddhiManager
				.addQuery("from gpsInputStream[geo:iswithin(lattitude, longitude, \"{ 'type': 'Polygon', 'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }\")==true] "
						+ "select 1 as iswithin " + "insert into gpsOutputStream;");

		withinFalseQueryReference = siddhiManager
				.addQuery("from gpsInputStream[geo:iswithin(lattitude, longitude, \"{ 'type': 'Polygon', 'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }\")==false] "
						+ "select 0 as iswithin " + "insert into gpsOutputStream;");
	}

	@Test
	public void testGeoIsWithinTrue() throws InterruptedException {
		siddhiManager.addCallback(withinTrueQueryReference,
				new QueryCallback() {
					@Override
					public void receive(long timeStamp, Event[] inEvents,
							Event[] removeEvents) {
						EventPrinter.print(timeStamp, inEvents, removeEvents);
						Assert.assertTrue((Integer)(inEvents[0].getData(0)) == 1);
					}
				});

		InputHandler inputHandler = siddhiManager
				.getInputHandler("gpsInputStream");
		inputHandler.send(new Object[] { 101.0, 0.5 });
	}

	@Test
	public void testGeoIsWithinFalse() throws InterruptedException {
		siddhiManager.addCallback(withinFalseQueryReference,
				new QueryCallback() {
					@Override
					public void receive(long timeStamp, Event[] inEvents,
							Event[] removeEvents) {
						EventPrinter.print(timeStamp, inEvents, removeEvents);
						Assert.assertTrue((Integer)(inEvents[0].getData(0)) == 0);
					}
				});

		InputHandler inputHandler = siddhiManager
				.getInputHandler("gpsInputStream");
		inputHandler.send(new Object[] { 101.0, 7.0 });
	}

	@After
	public void cleanUpEnviornment() {
		siddhiManager.shutdown();
	}
}
