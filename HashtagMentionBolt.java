package com.faiz.storm;


import java.util.Map;

import twitter4j.UserMentionEntity;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.regex.*; //use regex package to simplify the mapper function

public class HashtagMentionBolt implements IRichBolt {
	/**
	 * 
	 */
	 
	  public String partyParser(String party){
		   String parsedParty = "";
		   party = party.trim().toLowerCase();
		   Pattern pattern = Pattern.compile("conservative|labour|snp|libdem|plaid|green|brexit|ukip|yorkshire|cpa|mrlp|thesdpuk|liberal|alliance|dup"); //lists all parties' most used keywords.
			Matcher matcher = pattern.matcher(party);
				if (matcher.find())  //if match with the list above, then it is a 'season'.
				{parsedParty = matcher.group();}
			return parsedParty;
			}
		  
	
	private OutputCollector collector;
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		Status tweet = (Status) tuple.getValueByField("tweet");
		//System.out.println("From HashtagFilterBolt - Tweet : " + tweet.getText());
		for (HashtagEntity hashtage : tweet.getHashtagEntities()) {	
			if (partyParser(hashtage.getText().toLowerCase())!=null && !partyParser(hashtage.getText().toLowerCase()).isEmpty()){
				System.out.println("From HashtagMentionBolt -> #" + hashtage.getText() + " -> Party: " + partyParser(hashtage.getText()));
				this.collector.emit(new Values(partyParser(hashtage.getText())));
			}
		}	
		
		for (UserMentionEntity userMention : tweet.getUserMentionEntities()) {	
			if (partyParser(userMention.getScreenName().toLowerCase())!=null && !partyParser(userMention.getScreenName().toLowerCase()).isEmpty()){
				System.out.println("From HashtagMentionBolt -> @" + userMention.getScreenName() + " -> Party: " + partyParser(userMention.getScreenName()));
				this.collector.emit(new Values(partyParser(userMention.getScreenName())));
			}
		}	
	}

	@Override
	public void cleanup() {
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