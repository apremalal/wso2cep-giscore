package org.wso2.cep.geo.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.wso2.cep.geo.CRSTransformProcessor;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

@RunWith(JUnit4.class)
public class CRSTransformProcessorTestCase {

	private SiddhiManager siddhiManager;
	private String queryReference;

	@Before
	public void setUpEnviornment() {
		SiddhiConfiguration conf = new SiddhiConfiguration();
		List<Class> classList = new ArrayList<Class>();
		classList.add(CRSTransformProcessor.class);
		conf.setSiddhiExtensions(classList);

		siddhiManager = new SiddhiManager();
		siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);
		siddhiManager.defineStream("define stream gpsInputStream (lattitude double, longitude double) ");

		queryReference =
		                 siddhiManager.addQuery("from gpsInputStream#transform.geo:crstransform(\"EPSG:4326\",\"EPSG:25829\",lattitude,longitude) "
		                                        + "select lattitude,longitude "
		                                        + "insert into geoStream;");
	}

	@Test
	public void testCRSTransform() throws InterruptedException {
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				Assert.assertTrue((Double) (inEvents[0].getData(0)) == 1505646.888236971);
			}
		});
		InputHandler inputHandler = siddhiManager.getInputHandler("gpsInputStream");
		inputHandler.send(new Object[] { 0.0, 0.0 });
	}

	@After
	public void cleanUpEnviornment() {
		siddhiManager.shutdown();
	}
}
