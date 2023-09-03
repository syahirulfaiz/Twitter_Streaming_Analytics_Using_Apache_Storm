package com.faiz.storm;

import backtype.storm.Config;
import backtype.storm.LocalClustesr;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology_ {
	
	public static void main(String[] args) throws Exception {
		
		try {
			String consumerKey = "...";
			String consumerSecret = "...";
			String accessToken = "...";
			String accessTokenSecret = "...";
			//String[] keyWords = {"#GE2019, #generalelection, #VoteConservative","#VoteLabour2019","#VoteSNP","#VoteLibDem","#HywelPlaid19","#VoteGreen","#VoteBrexitParty"}; //Major 7 parties. https://en.wikipedia.org/wiki/2019_United_Kingdom_general_election#Predictions_three_weeks_before_the_vote
			String[] keyWords = {"#GE2019, #generalelection"};
			Config config = new Config();
			//config.setDebug(true);
			//config.setDebug(false);
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("Spout_", new Spout_(consumerKey,
					consumerSecret, accessToken, accessTokenSecret, keyWords),3);
			builder.setBolt("HashtagMentionBolt_", new HashtagMentionBolt_(),2)
					.shuffleGrouping("Spout_");
			builder.setBolt("CounterBolt_",
					new CounterBolt_(),1).fieldsGrouping(
					"HashtagMentionBolt_", new Fields("hashtag"));
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Topology_", config,
					builder.createTopology());
			Thread.sleep(20000);
			//Thread.sleep(1200000); //20 mins
			//Thread.sleep(1800000); //0.5 hours
			//Thread.sleep(2700000); //45 mins
			//Thread.sleep(3600000); //1 hours
			//Thread.sleep(7200000); //2 hours
			//Thread.sleep(10800000); //3 hours
			cluster.shutdown();
		}catch(Exception e) {
			System.out.println("\nApplication Terminates. Please see 'Result' above. Thank you for using this application.");
		}
		
		
	}
}