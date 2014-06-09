package org.wso2.cep.extension.gis;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class Test {
	public static void main(String[] args) throws Exception {
		// log.info("Filter test119");
		SiddhiConfiguration conf = new SiddhiConfiguration();
		List<Class> classList = new ArrayList<Class>();
		classList.add(GisWithin.class);
		conf.setSiddhiExtensions(classList);
		
		SiddhiManager siddhiManager = new SiddhiManager();
		siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);
		System.out.println(siddhiManager.getSiddhiContext().getSiddhiExtensions().toString());
		
		siddhiManager
				.defineStream("define stream cseEventStream (symbol1 string, symbol2 string, lattitude long, longitude long) ");

		String queryReference = siddhiManager
				.addQuery("from cseEventStream "
						+ "select symbol1, symbol2, gis:iswithin(\"{ 'type': 'Point', 'coordinates': [100.5, 0.5] }\",\"{ 'type': 'Polygon', 'coordinates': [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }\") as tt "
						+ "insert into StockQuote;");

		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents,
					Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				// Assert.assertTrue("IBM".equals(inEvents[0].getData(0)) ||
				// "WSO2".equals(inEvents[0].getData(0)));
				// count++;
				// eventArrived = true;
			}
		});
		InputHandler inputHandler = siddhiManager
				.getInputHandler("cseEventStream");
		inputHandler.send(new Object[] { "I", "BM", 756l, 100l });
		inputHandler.send(new Object[] { "IBM", "", 756l, 25l });
		inputHandler.send(new Object[] { "IB", "M", 756l, 100l });
		Thread.sleep(100);
		// Assert.assertEquals(3, count);
		// Assert.assertEquals("Event arrived", true, eventArrived);
		siddhiManager.shutdown();
		//gis:within(symbol1,symbol2)

	}
}
