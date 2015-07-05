import java.io.IOException;
import java.util.HashMap;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

  public class MapClass extends Mapper<LongWritable,Text,Text,Mutation> {
	  
	HashMap<String, String> teamMap = new HashMap<String, String>();
	HashMap<String, String> regionMap = new HashMap<String, String>();
	
    private String fileName = "";
    private int wins = 0;
    private int loses= 0;
    
	@Override
    protected void setup(Context context)
    		throws IOException, InterruptedException {
    	fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
    	teamMap.put("Celtics","Boston Celtics|east");
    	teamMap.put("Knicks","New York Knicks|east");
    	teamMap.put("76ers","Philadelphia 76ers|east");
    	teamMap.put("Nets","New Jersey Nets|east");
    	teamMap.put("Raptors","Toronto Raptors|east");
    	teamMap.put("Bulls","Chicago Bulls|east");
    	teamMap.put("Pacers","Indiana Pacers|east");
    	teamMap.put("Bucks","Milwaukee Bucks|east");
    	teamMap.put("Pistons","Detroit Pistons|east");
    	teamMap.put("Cavs","Cleveland Cavaliers|east");
    	teamMap.put("MiamiHeat","Miami Heat|east");
    	teamMap.put("OrlandoMagi","Orlando Magic|east");
    	teamMap.put("Hawks","Atlanta Hawks|east");
    	teamMap.put("Bobcats","Charlotte Bobcats|east");
    	teamMap.put("Wizards","Washington Wizards|east");
    	
    	teamMap.put("okcthunder","Oklahoma City|west");
    	teamMap.put("Nuggets","Denver Nuggets|west");
    	teamMap.put("TrailBlazer","Portland Trailblazers|west");
    	teamMap.put("UtahJazz","Utah Jazz|west");
    	teamMap.put("TWolves","Minnesota Timberwolves|west");
    	teamMap.put("Lakers","LA Lakers|west");
    	teamMap.put("Suns","Phoenix Suns|west");
    	teamMap.put("GSWarriors","Golden State Warriors|west");
    	teamMap.put("Clippers","L.A. Clippers|west");
    	teamMap.put("NBAKings","Sacramento Kings|west");
    	teamMap.put("GoSpursGo","San Antonio Spurs|west");
    	teamMap.put("Mavs","Dallas Mavericks|west");
    	teamMap.put("Hornets","New Orleans Hornets|west");
    	teamMap.put("Grizzlies","Memphis Grizzlies|west");
    	teamMap.put("Rockets","Houston Rockets|west");
    	
    	regionMap.put("okcthunder", "west");
    	regionMap.put("Nuggets", "west");
    	regionMap.put("TrailBlazer", "west");
    	regionMap.put("UtahJazz", "west");
    	regionMap.put("TWolves", "west");
    	regionMap.put("Lakers", "west");
    	regionMap.put("Suns", "west");
    	regionMap.put("GSWarriors", "west");
    	regionMap.put("NBAKings", "west");
    	regionMap.put("GoSpursGo", "west");
    	regionMap.put("Mavs", "west");
    	regionMap.put("Grizzlies", "west");
    	regionMap.put("Hornets", "west");
    	regionMap.put("Rockets", "west");
    	regionMap.put("Clippers", "west");
    	
    	regionMap.put("Celtics", "east");
    	regionMap.put("Knicks", "east");
    	regionMap.put("76ers", "east");
    	regionMap.put("Nets", "east");
    	regionMap.put("Raptors", "east");
    	regionMap.put("Bulls", "east");
    	regionMap.put("Pacers", "east");
    	regionMap.put("Bucks", "east");
    	regionMap.put("Pistons", "east");
    	regionMap.put("Cavs", "east");
    	regionMap.put("MiamiHeat", "east");
    	regionMap.put("OrlandoMagi", "east");
    	regionMap.put("Hawks", "east");
    	regionMap.put("Bobcats", "east");
    	regionMap.put("Wizards", "east");    
    	
    }
	  
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException {
      String[] words = value.toString().trim().split("\\s+");
      
      for (String word : words) {
    	  word = word.replaceAll("\\W", "");
    	  if(word.toLowerCase().equals("win"))		wins++;    	  
    	  
		  else if(word.toLowerCase().equals("lose")) 	loses++;
      }
    }
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		String teamName = teamMap.get(fileName);
		String region = regionMap.get(fileName);
		String loseTable = "lose"; //Create Table eastLOSE, eastWIN, westLOSE, westWIN in accumulo
		String winTable = "win";

		Mutation winMut = new Mutation(new Text(teamName));
		Mutation loseMut = new Mutation(new Text(teamName));
		
		winMut.put(new Text("WinLoses"), new Text("NBATeam"), new ColumnVisibility(region), new Value(Integer.toString(wins).getBytes()));
		loseMut.put(new Text("WinLoses"), new Text("NBATeam"),new ColumnVisibility(region),  new Value(Integer.toString(loses).getBytes()));
		context.write(new Text(winTable), winMut);
		context.write(new Text(loseTable), loseMut);
	}
  }