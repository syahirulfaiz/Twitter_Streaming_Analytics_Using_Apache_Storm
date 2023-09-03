package com.faiz.storm;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.tuple.Fields;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class CounterBolt implements IRichBolt {
	Map<String, Integer> counterMap;
	private OutputCollector collector;
	private int sum=0;
	private float probability=0;
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.counterMap = new HashMap<String, Integer>();
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String key = tuple.getString(0);
		if (!counterMap.containsKey(key)) {
			counterMap.put(key, 1);
		} else {
			Integer c = counterMap.get(key) + 1;
			counterMap.put(key, c);
		}
		collector.ack(tuple);
		sum=sum+1;
	}

	@Override
	public void cleanup() {
		for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
			DecimalFormat df = new DecimalFormat("#%");
			probability = (float) entry.getValue()/sum;
			System.out.println("From CounterBolt -> Result: " + entry.getKey() + " : "
					+ entry.getValue() + " from total : "+sum+ " keywords -> Probability : "+ probability + " - ("+ df.format(probability) +")");
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}