import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Scanner;
import static java.lang.Integer.parseInt;
import static java.lang.Float.parseFloat;

public class Project {
    static Connection connection;

    public static void main(String[] args) throws Exception {

	// startup sequence
	MLBDatabase db = new MLBDatabase();
	runConsole(db);

	System.out.println("Exiting...");
    }

    public static void runConsole(MLBDatabase db) {

	Scanner console = new Scanner(System.in);
	System.out.print("Welcome! Type h for help. ");
	System.out.print("db > ");
	String line = console.nextLine();
	String[] parts;
	String arg = "";

	while (line != null && !line.equals("q")) {
		parts = line.split("\\s+");
		if (line.indexOf(" ") > 0)
		    arg = line.substring(line.indexOf(" ")).trim();

		if (parts[0].equals("h"))
		    printHelp();
		else if (parts[0].equals("pop"))
		    db.populateData();
		else if (parts[0].equals("bday")) {
		    try {
		        if (parts.length == 3)
			    db.bdaySearch(parseInt(parts[1]), parseInt(parts[2]));
			else
			    System.out.println("Require 2 arguments for this query");
		    } catch (Exception e) {
			System.out.println("Month and Day both must be integers");
		    }
		}
		else if (parts[0].equals("pwins"))
		    db.pitchingCareerWins();
		else if (parts[0].equals("chips"))
		    db.numChampionships();
		else if (parts[0].equals("winTeam")) {
		    try {
			if (parts.length == 2)
			    db.winningTeams(parseInt(parts[1]));
			else
			    System.out.println("Require 1 argument for this query");
		    } catch (Exception e) {
			System.out.println("year must be an integer");
		    }
		}
                else if (parts[0].equals("sr")) {
                    try {
                        if (parts.length > 1)
                            db.searchPlayers(arg);
                        else
                            System.out.println("Require at least 1 argument for this query");
                    } catch (Exception e) {
                        System.out.println("Name must be a string");
                    }
                }
                else if (parts[0].equals("pos")) {
                    try {
                        if (parts.length == 2)
                            db.playerPositions(parts[1]);
                        else
                            System.out.println("Require 1 argument for this query");
                    } catch (Exception e) {
                        System.out.println("playerID must be a string");
                    }
                }
		else if (parts[0].equals("numSchools"))
		    db.numSchoolsAttended();
		else if (parts[0].equals("top10ActivePlayers"))
		    db.top10ActivePlayers();
		else if (parts[0].equals("multiNation"))
		    db.multiNationPlayers();

		System.out.print("db > ");
		line = console.nextLine();
	}

	console.close();
    }

    private static void printHelp() {
	System.out.println("MLB database");
	System.out.println("Commands:");
	System.out.println("h - Get help");
	System.out.println("");
	System.out.println("pop - populate the data");
	System.out.println("bday <month> <day> - Search all players with this birthday");
	System.out.println("pwins - Find the top 10 pitchers in career wins");
	System.out.println("chips - Find all franchises that have won a championship and how many championships they have won");
	System.out.println("winTeam <year> - Find all winning record teams and players who played on the teams from the given\n\tyear and report the teams with a winning record and their W/L record regardless\n\tof roster or salary information being available. If roster and/or salary information \n\tis available report it (salary information begins being available in 1985).");	
	System.out.println("sr <name> - Find all players who have their name that matches the search.\n\tCan use first, middle, and last name case insensitive able to use partial name,\n\tbut to use partial name for middle while inputting first need full first\n\t. Report their playerID, full name, debut date and final game date.");
	System.out.println("numSchools - Print out each player firstname, lastname and total number \n\tof schools that player attended.");
	System.out.println("top10ActivePlayers - Which players has the highest number of games played. \n\tReport  top 10 players firstname, lastname and their number of games played.");
	System.out.println("multiNation - Search for players who were born and studied in different countries. \n\tPrint out each player firstname, last name, their birth country and the \n\tschool they attended that is different from where they were born, \n\tand how many countries they studied that different from their birth country.");
	System.out.println("pos <playerID> - Search for a player using their playerID and report their name, \n\twhich hand they throw with as R(right), L(left), or S(both), \n\tall of the positions they have played, and how many years they played \n\tthat position (They could play more than one position a year).");
	System.out.println("q - Exit the program");

	System.out.println("---- end help ----- ");
    }

}

class MLBDatabase {
    private Connection connection;

    public MLBDatabase() {
	try {
	    Properties prop = new Properties();
            String fileName = "auth.cfg";
            try {
                FileInputStream configFile = new FileInputStream(fileName);
                prop.load(configFile);
                configFile.close();
            } catch (FileNotFoundException ex) {
                System.out.println("Could not find config file.");
                System.exit(1);
            } catch (IOException ex) {
                System.out.println("Error reading config file.");
                System.exit(1);
            }
            String username = (prop.getProperty("username"));
            String password = (prop.getProperty("password"));

            if (username == null || password == null){
                System.out.println("Username or password not provided.");
                System.exit(1);
            }

            String connectionUrl =
                    "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                    + "database=cs3380;"
                    + "user=" + username + ";"
                    + "password="+ password +";"
                    + "encrypt=false;"
                    + "trustServerCertificate=false;"
                    + "loginTimeout=30;";
   	    // create a connection to the database
	    connection = DriverManager.getConnection(connectionUrl);
	} catch (SQLException e) {
	    e.printStackTrace(System.out);
	}
    }

    public void bdaySearch(int month, int day) {
	try {
		String sql = "select players.playerid, firstname, lastname, birthyear, birthcountry from players where birthmonth = ? and birthday = ?;";

		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setInt(1, month);
		statement.setInt(2, day);
		ResultSet resultSet = statement.executeQuery();

		while (resultSet.next()) {
		    System.out.println(resultSet.getString("playerID") + " - " + resultSet.getString("firstname") + " " + resultSet.getString("lastname") + ", " + resultSet.getInt("birthyear") + ", " + resultSet.getString("birthcountry"));
		}

		resultSet.close();
		statement.close();
	} catch (SQLException e) {
		e.printStackTrace(System.out);
	}
    }

    public void pitchingCareerWins() {
	try {
		String sql = "with careerWins as (select top 10 players.playerid, players.firstname, players.lastname, sum(pitching.w) as totalWins from pitching inner join players on players.playerid=pitching.playerid group by players.playerID, players.firstname, players.lastname order by sum(pitching.w) DESC) select firstname, lastname, totalWins from careerWins;";

		PreparedStatement statement = connection.prepareStatement(sql);
		ResultSet resultSet = statement.executeQuery();

		while (resultSet.next()) {
				System.out.println(resultSet.getString("firstname") + " " + resultSet.getString("lastname") + ", " + resultSet.getInt("totalWins"));
		}

		resultSet.close();
		statement.close();
	} catch (SQLException e) {
		e.printStackTrace(System.out);
	}
    }

    public void numChampionships() {
	try {
	    String sql = "select teamFranchises.franchName, COUNT(teams.LgWin) as TotalChampionshipTitles from teams join teamFranchises on teamFranchises.franchID = teams.franchID where teams.LgWin = 'Y' group by teams.franchID, franchName order by TotalChampionshipTitles DESC;";

	    PreparedStatement statement = connection.prepareStatement(sql);
	    ResultSet resultSet = statement.executeQuery();

	    while (resultSet.next()) {
		System.out.println(resultSet.getString("franchName") + " - " + resultSet.getString("TotalChampionshipTitles"));
	    }

	    resultSet.close();
	    statement.close();
	} catch (SQLException e) {
	    e.printStackTrace(System.out);
	}
    }

    public void winningTeams(int year) {
	try {
  	    String sql = "Select teamname, teams.yearID, w, l, firstname, lastname, salary from teams left join salaries on teams.teamId = salaries.teamID and teams.yearID = salaries.yearID and teams.lgID = salaries.lgID left join players on salaries.playerID = players.playerID where w>l and teams.yearID = ? order by teams.teamId;";

	    PreparedStatement statement = connection.prepareStatement(sql);
	    statement.setInt(1,year);
	    ResultSet resultSet = statement.executeQuery();

	    while (resultSet.next()) {
		System.out.println(resultSet.getString("teamname") + " " + resultSet.getString("yearID") + ":  Wins: " + resultSet.getInt("w") + " Losses: " + resultSet.getInt("l") + " Player: " + resultSet.getString("firstname") + " " + resultSet.getString("lastname") + " Salary: " + resultSet.getInt("salary"));
	    }

	    resultSet.close();
	    statement.close();
	} catch (SQLException e) {
		e.printStackTrace(System.out);
	}
    }

    public void homeRunLeader() {
        try{
            String sql="with hrleader as (select batting.yearID,min(players.playerid) as playerID, max(HR) as highestHomeRun from batting inner join players on players.playerid=batting.playerid group by batting.yearID)  select hrleader.yearid, firstname, lastname, hrleader.highestHomeRun from hrleader left join players on players.playerid=hrleader.playerid order by hrleader.yearid";

            PreparedStatement statement =connection.prepareStatement(sql);
            ResultSet resultSet=statement.executeQuery();

	    while (resultSet.next()) {
                System.out.println(resultSet.getInt("yearID")+", "+resultSet.getString("firstname")+", "+resultSet.getInt("lastname")+", " +resultSet.getInt("highestHomeRun"));
	    }

            resultSet.close();
            statement.close();

  	} catch (SQLException e) {
	    e.printStackTrace(System.out);
	}
    }

    public void playerPositions(String id) {
	try {
	    String sql = "select players.firstname, players.lastname, players.throw, fielding.POS, COUNT(yearID) as numYears from players join fielding on players.playerID = fielding.playerID where players.playerID = ? group by fielding.POS, players.firstname, players.lastname, players.throw;";

	    PreparedStatement statement = connection.prepareStatement(sql);
	    statement.setString(1,id);
 	    ResultSet resultSet = statement.executeQuery();

	    while (resultSet.next()) {
		System.out.println(resultSet.getString("firstname") + " " + resultSet.getString("lastname") +  " THROWS: " + resultSet.getString("throw") + " POSITION: " + resultSet.getString("POS") + " NUMBER OF YEARS: " + resultSet.getInt("numYears"));
	    }

	    resultSet.close();
	    statement.close();
	} catch (SQLException e) {
	    e.printStackTrace(System.out);
	}
    }

    public void populateData() {

        try {

	    System.out.println("Deleting existing data...");

	    String deleteCollegePlaying = "DELETE FROM collegePlaying;";
	    String deletePitching = "DELETE FROM pitching;";
	    String deleteAppearances = "DELETE FROM appearances;";
	    String deleteFielding = "DELETE FROM fielding;";
	    String deleteBatting = "DELETE FROM batting;";
	    String deleteSalaries = "DELETE FROM salaries;";
	    String deleteTeamFranchises = "DELETE FROM teamFranchises;";
	    String deletePlayers = "DELETE FROM players;";
	    String deleteTeams = "DELETE FROM teams;";
	    String deleteSchools = "DELETE FROM schools;";

	    PreparedStatement deleteTables = connection.prepareStatement(deleteCollegePlaying+deletePitching+deleteAppearances+deleteFielding+deleteBatting+deleteSalaries+deleteTeamFranchises+deletePlayers+deleteTeams+deleteSchools);

	    deleteTables.executeUpdate();

	    System.out.println("Deletion complete.");
	    System.out.println("Inserting players...");
	
            String insertPlayers = "INSERT INTO players values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            PreparedStatement populatePlayers = connection.prepareStatement(insertPlayers);

            BufferedReader playerReader = new BufferedReader(new FileReader("Players.csv"));

            String line = null;
            String[] player;

            playerReader.readLine();

            while((line = playerReader.readLine()) != null) {
                player = line.split(",");

                for(int i = 0; i < player.length; i++) {
                    if(player[i].isEmpty())
                        player[i] = null;
                }

                populatePlayers.setString(1,player[0]);
                if(player[1] == null)
                    populatePlayers.setNull(2,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(2,parseInt(player[1]));
                if(player[2] == null)
                    populatePlayers.setNull(3,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(3,parseInt(player[2]));
                if(player[3] == null)
                    populatePlayers.setNull(4,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(4,parseInt(player[3]));
                populatePlayers.setString(5,player[4]);
                populatePlayers.setString(6,player[5]);
                populatePlayers.setString(7,player[6]);
                if(player[7] == null)
                    populatePlayers.setNull(8,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(8,parseInt(player[7]));
                if(player[8] == null)
                    populatePlayers.setNull(9,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(9,parseInt(player[8]));
                if(player[9] == null)
                    populatePlayers.setNull(10,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(10,parseInt(player[9]));
                populatePlayers.setString(11,player[10]);
                populatePlayers.setString(12,player[11]);
                populatePlayers.setString(13,player[12]);
                populatePlayers.setString(14,player[13]);
                populatePlayers.setString(15,player[14]);
                populatePlayers.setString(16,player[15]);
                if(player[16] == null)
                    populatePlayers.setNull(17,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(17,parseInt(player[16]));
                if(player[17] == null)
                    populatePlayers.setNull(18,java.sql.Types.INTEGER);
                else
                    populatePlayers.setInt(18,parseInt(player[17]));
                populatePlayers.setString(19,player[18]);
                populatePlayers.setString(20,player[19]);
                populatePlayers.setString(21,player[20]);
                populatePlayers.setString(22,player[21]);

                populatePlayers.executeUpdate();
            }
            playerReader.close();
            connection.commit();

	    System.out.println("Inserting players complete.");
	    System.out.println("Inserting teams...");

            String insertTeams = "INSERT INTO teams values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            PreparedStatement populateTeams = connection.prepareStatement(insertTeams);

            BufferedReader teamReader = new BufferedReader(new FileReader("Teams.csv"));

            String[] team;

            teamReader.readLine();

            while((line = teamReader.readLine()) != null) {
                team = line.split(",");

                for(int i = 0; i < team.length; i++) {
                    if(team[i].isEmpty())
                        team[i] = null;
                }

                populateTeams.setInt(1,parseInt(team[0]));
                populateTeams.setString(2,team[1]);
                populateTeams.setString(3,team[2]);
                populateTeams.setString(4,team[3]);
                populateTeams.setString(5,team[4]);
                populateTeams.setInt(6,parseInt(team[5]));
                populateTeams.setInt(7,parseInt(team[6]));
                if(team[7] == null)
                    populateTeams.setNull(8,java.sql.Types.INTEGER);
                else
                    populateTeams.setInt(8,parseInt(team[7]));
                populateTeams.setInt(9,parseInt(team[8]));
                populateTeams.setInt(10,parseInt(team[9]));
                populateTeams.setString(11,team[10]);
                populateTeams.setString(12,team[11]);
                populateTeams.setString(13,team[12]);
                populateTeams.setString(14,team[13]);
                populateTeams.setInt(15,parseInt(team[14]));
                populateTeams.setInt(16,parseInt(team[15]));
                populateTeams.setInt(17,parseInt(team[16]));
                populateTeams.setInt(18,parseInt(team[17]));
                populateTeams.setInt(19,parseInt(team[18]));
                populateTeams.setInt(20,parseInt(team[19]));
                populateTeams.setInt(21,parseInt(team[20]));
                populateTeams.setInt(22,parseInt(team[21]));
                populateTeams.setInt(23,parseInt(team[22]));
                populateTeams.setInt(24,parseInt(team[23]));
                populateTeams.setInt(25,parseInt(team[24]));
                populateTeams.setInt(26,parseInt(team[25]));
                populateTeams.setInt(27,parseInt(team[26]));
                populateTeams.setInt(28,parseInt(team[27]));
                populateTeams.setFloat(29,parseFloat(team[28]));
                populateTeams.setInt(30,parseInt(team[29]));
                populateTeams.setInt(31,parseInt(team[30]));
                populateTeams.setInt(32,parseInt(team[31]));
                populateTeams.setInt(33,parseInt(team[32]));
                populateTeams.setInt(34,parseInt(team[33]));
                populateTeams.setInt(35,parseInt(team[34]));
                populateTeams.setInt(36,parseInt(team[35]));
                populateTeams.setInt(37,parseInt(team[36]));
                populateTeams.setInt(38,parseInt(team[37]));
                populateTeams.setInt(39,parseInt(team[38]));
                populateTeams.setFloat(40,parseFloat(team[39]));
                populateTeams.setString(41,team[40]);
                if(team[41] == null)
                     populateTeams.setNull(42,java.sql.Types.INTEGER);
                 else
                    populateTeams.setInt(42,parseInt(team[41]));
                populateTeams.setInt(43,parseInt(team[42]));
                populateTeams.setInt(44,parseInt(team[43]));

                populateTeams.executeUpdate();
            }
            teamReader.close();
            connection.commit();

            System.out.println("Inserting teams complete.");
            System.out.println("Inserting teamFranchises...");

            String insertTeamsFranchises = "INSERT INTO teamFranchises values(?,?,?,?);";

            PreparedStatement populateTeamsFranchises = connection.prepareStatement(insertTeamsFranchises);

            BufferedReader teamFranchisesReader = new BufferedReader(new FileReader("TeamsFranchises.csv"));

            String[] teamFranchises;

            teamFranchisesReader.readLine();

            while((line = teamFranchisesReader.readLine()) != null) {
                teamFranchises = line.split(",");

                for(int i = 0; i < teamFranchises.length; i++) {
                    if(teamFranchises[i].isEmpty())
                        teamFranchises[i] = null;
                }

                populateTeamsFranchises.setString(1,teamFranchises[0]);
                populateTeamsFranchises.setString(2,teamFranchises[1]);
                populateTeamsFranchises.setString(3,teamFranchises[2]);
                if(4 == teamFranchises.length) //since last data in csv can be null array might not be length 4.
		    populateTeamsFranchises.setString(4,teamFranchises[3]);
		else
		    populateTeamsFranchises.setString(4,null);

                populateTeamsFranchises.executeUpdate();
            }
            teamFranchisesReader.close();
            connection.commit();

            System.out.println("Inserting teamFranchises complete.");
            System.out.println("Inserting schools...");

            String insertSchools = "INSERT INTO schools values(?,?,?,?,?);";

            PreparedStatement populateSchools = connection.prepareStatement(insertSchools);

            BufferedReader schoolsReader = new BufferedReader(new FileReader("Schools.csv"));

            String[] schools;

            schoolsReader.readLine();

            while((line = schoolsReader.readLine()) != null) {
                schools = line.split(",");

                for(int i = 0; i < schools.length; i++) {
                    if(schools[i].isEmpty())
                        schools[i] = null;
                }

                populateSchools.setString(1,schools[0]);
                populateSchools.setString(2,schools[1]);
                populateSchools.setString(3,schools[2]);
                populateSchools.setString(4,schools[3]);
                populateSchools.setString(5,schools[4]);

                populateSchools.executeUpdate();
            }
            schoolsReader.close();
            connection.commit();

            System.out.println("Inserting schools complete.");
            System.out.println("Inserting collegePlaying...");

            String insertCollegePlaying = "INSERT INTO collegePlaying values(?,?,?);";

            PreparedStatement populateCollegePlaying = connection.prepareStatement(insertCollegePlaying);

            BufferedReader collegePlayingReader = new BufferedReader(new FileReader("CollegePlaying.csv"));

            String[] collegePlaying;

            collegePlayingReader.readLine();

            while((line = collegePlayingReader.readLine()) != null) {
                collegePlaying = line.split(",");

                populateCollegePlaying.setString(1,collegePlaying[0]);
                populateCollegePlaying.setString(2,collegePlaying[1]);
                populateCollegePlaying.setInt(3,parseInt(collegePlaying[2]));

                populateCollegePlaying.executeUpdate();
            }
            collegePlayingReader.close();
            connection.commit();

            System.out.println("Inserting collegePlaying complete.");
            System.out.println("Inserting salaries...");

            String insertSalaries = "INSERT INTO salaries values(?,?,?,?,?);";

            PreparedStatement populateSalaries = connection.prepareStatement(insertSalaries);

            BufferedReader salariesReader = new BufferedReader(new FileReader("Salaries.csv"));

            String[] salaries;

            salariesReader.readLine();

            while((line = salariesReader.readLine()) != null) {
                salaries = line.split(",");

                populateSalaries.setInt(1,parseInt(salaries[0]));
                populateSalaries.setString(2,salaries[1]);
                populateSalaries.setString(3,salaries[2]);
                populateSalaries.setString(4,salaries[3]);
                populateSalaries.setInt(5,parseInt("0"+salaries[4]));

                populateSalaries.executeUpdate();
            }
            salariesReader.close();
            connection.commit();

            System.out.println("Inserting salaries complete.");
            System.out.println("Inserting pitching...");

            String insertPitching = "INSERT INTO pitching values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            PreparedStatement populatePitching = connection.prepareStatement(insertPitching);

            BufferedReader pitchingReader = new BufferedReader(new FileReader("Pitching.csv"));

            String[] pitching;

            pitchingReader.readLine();

            while((line = pitchingReader.readLine()) != null) {
                pitching = line.split(",");

                for(int i = 0; i < pitching.length; i++) {
                    if(pitching[i].isEmpty())
                        pitching[i] = null;
                }

                populatePitching.setString(1,pitching[0]);
                populatePitching.setInt(2,parseInt(pitching[1]));
                populatePitching.setInt(3,parseInt(pitching[2]));
                populatePitching.setString(4,pitching[3]);
                populatePitching.setString(5,pitching[4]);
                populatePitching.setInt(6,parseInt(pitching[5]));
                populatePitching.setInt(7,parseInt(pitching[6]));
                populatePitching.setInt(8,parseInt(pitching[7]));
                populatePitching.setInt(9,parseInt(pitching[8]));
                populatePitching.setInt(10,parseInt(pitching[9]));
                populatePitching.setInt(11,parseInt(pitching[10]));
                populatePitching.setInt(12,parseInt(pitching[11]));
                populatePitching.setInt(13,parseInt(pitching[12]));
                populatePitching.setInt(14,parseInt(pitching[13]));
                populatePitching.setInt(15,parseInt(pitching[14]));
                populatePitching.setInt(16,parseInt(pitching[15]));
                populatePitching.setInt(17,parseInt(pitching[16]));
                populatePitching.setInt(18,parseInt(pitching[17]));
                if(pitching[18] == null)   
                    populatePitching.setNull(19,java.sql.Types.FLOAT);
                else
                    populatePitching.setFloat(19,parseFloat(pitching[18]));
                if(pitching[19] == null)
                     populatePitching.setNull(20,java.sql.Types.FLOAT);
                 else
                    populatePitching.setFloat(20,parseFloat(pitching[19]));
                if(pitching[20] == null)   
                    populatePitching.setNull(21,java.sql.Types.INTEGER);
                else
                    populatePitching.setInt(21,parseInt(pitching[20]));
                populatePitching.setInt(22,parseInt(pitching[21]));
                if(pitching[18] == null)   
                    populatePitching.setNull(23,java.sql.Types.INTEGER);
                else
                    populatePitching.setInt(23,parseInt(pitching[22]));
                populatePitching.setInt(24,parseInt(pitching[23]));
                if(pitching[24] == null)
                    populatePitching.setNull(25,java.sql.Types.INTEGER);
                else
		    populatePitching.setInt(25,parseInt(pitching[24]));
                populatePitching.setInt(26,parseInt(pitching[25]));
                populatePitching.setInt(27,parseInt(pitching[26]));

                populatePitching.executeUpdate();
            }
            pitchingReader.close();
            connection.commit();

            System.out.println("Inserting pitching complete.");
            System.out.println("Inserting batting...");

            String insertBatting = "INSERT INTO batting values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            PreparedStatement populateBatting = connection.prepareStatement(insertBatting);

            BufferedReader battingReader = new BufferedReader(new FileReader("Batting.csv"));

            String[] batting;

            battingReader.readLine();

            while((line = battingReader.readLine()) != null) {
                batting = line.split(",");

                populateBatting.setString(1,batting[0]);
                populateBatting.setInt(2,parseInt(batting[1]));
                populateBatting.setInt(3,parseInt(batting[2]));
                populateBatting.setString(4,batting[3]);
                populateBatting.setString(5,batting[4]);
                populateBatting.setInt(6,parseInt(batting[5]));
                populateBatting.setInt(7,parseInt(batting[6]));
                populateBatting.setInt(8,parseInt(batting[7]));
                populateBatting.setInt(9,parseInt(batting[8]));
                populateBatting.setInt(10,parseInt(batting[9]));
                populateBatting.setInt(11,parseInt(batting[10]));
                populateBatting.setInt(12,parseInt(batting[11]));
                populateBatting.setInt(13,parseInt(batting[12]));
                populateBatting.setInt(14,parseInt(batting[13]));
                populateBatting.setInt(15,parseInt(batting[14]));
                populateBatting.setInt(16,parseInt(batting[15]));
                populateBatting.setInt(17,parseInt(batting[16]));
                populateBatting.setInt(18,parseInt(batting[17]));
                populateBatting.setInt(19,parseInt(batting[18]));
                populateBatting.setInt(20,parseInt(batting[19]));
                populateBatting.setInt(21,parseInt(batting[20]));
                populateBatting.setInt(22,parseInt(batting[21]));

                populateBatting.executeUpdate();
            }
            battingReader.close();
            connection.commit();

            System.out.println("Inserting batting complete.");
            System.out.println("Inserting appearances...");

            String insertAppearances = "INSERT INTO appearances values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

            PreparedStatement populateAppearances = connection.prepareStatement(insertAppearances);

            BufferedReader appearancesReader = new BufferedReader(new FileReader("Appearances.csv"));

            String[] appearances;

            appearancesReader.readLine();

            while((line = appearancesReader.readLine()) != null) {
                appearances = line.split(",");

                populateAppearances.setInt(1,parseInt(appearances[0]));
                populateAppearances.setString(2,appearances[1]);
                populateAppearances.setString(3,appearances[2]);
                populateAppearances.setString(4,appearances[3]);
                populateAppearances.setInt(5,parseInt(appearances[4]));
                populateAppearances.setInt(6,parseInt(appearances[5]));
                populateAppearances.setInt(7,parseInt(appearances[6]));
                populateAppearances.setInt(8,parseInt(appearances[7]));
                populateAppearances.setInt(9,parseInt(appearances[8]));
                populateAppearances.setInt(10,parseInt(appearances[9]));
                populateAppearances.setInt(11,parseInt(appearances[10]));
                populateAppearances.setInt(12,parseInt(appearances[11]));
                populateAppearances.setInt(13,parseInt(appearances[12]));
                populateAppearances.setInt(14,parseInt(appearances[13]));
                populateAppearances.setInt(15,parseInt(appearances[14]));
                populateAppearances.setInt(16,parseInt(appearances[15]));
                populateAppearances.setInt(17,parseInt(appearances[16]));
                populateAppearances.setInt(18,parseInt(appearances[17]));
                populateAppearances.setInt(19,parseInt(appearances[18]));
                populateAppearances.setInt(20,parseInt(appearances[19]));
                populateAppearances.setInt(21,parseInt(appearances[20]));

                populateAppearances.executeUpdate();
            }
            appearancesReader.close();
            connection.commit();

            System.out.println("Inserting appearances complete.");
            System.out.println("Inserting fielding...");

            String insertFielding = "INSERT INTO fielding values(?,?,?,?,?,?,?,?,?,?,?,?,?);";

            PreparedStatement populateFielding = connection.prepareStatement(insertFielding);

            BufferedReader fieldingReader = new BufferedReader(new FileReader("Fielding.csv"));

            String[] fielding;

            fieldingReader.readLine();

            while((line = fieldingReader.readLine()) != null) {
                fielding = line.split(",");

                for(int i = 0; i < fielding.length; i++) {
                    if(fielding[i].isEmpty())
                        fielding[i] = null;
                }

                populateFielding.setString(1,fielding[0]);
                populateFielding.setInt(2,parseInt(fielding[1]));
                populateFielding.setInt(3,parseInt(fielding[2]));
                populateFielding.setString(4,fielding[3]);
                populateFielding.setString(5,fielding[4]);
                populateFielding.setString(6,fielding[5]);
                populateFielding.setInt(7,parseInt(fielding[6]));
                if(fielding[7] == null)
                    populateFielding.setNull(8,java.sql.Types.INTEGER);
                else
		    populateFielding.setInt(8,parseInt(fielding[7]));
                if(fielding[8] == null)
                    populateFielding.setNull(9,java.sql.Types.INTEGER);
                else
		    populateFielding.setInt(9,parseInt(fielding[8]));
                populateFielding.setInt(10,parseInt(fielding[9]));
                populateFielding.setInt(11,parseInt(fielding[10]));
                if(fielding[11] == null)
                    populateFielding.setNull(12,java.sql.Types.INTEGER);
                else
		    populateFielding.setInt(12,parseInt(fielding[11]));
                populateFielding.setInt(13,parseInt(fielding[12]));

                populateFielding.executeUpdate();
            }
            fieldingReader.close();
            connection.commit();

            System.out.println("Inserting fielding complete.");
        }
        catch (FileNotFoundException f) {
            f.printStackTrace();
        }
        catch (IOException io) {
            io.printStackTrace();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
	


//////////////////////////////////////////////////////////
//Find all players who have their name that matches the given \n\tname. Report their playerID, full name, debut date and final game date.
    public void searchPlayers(String name)
    {
		name=name.toUpperCase();
    		ResultSet resultSet;
		String[] splitName = name.split("\\s+");
		String givenName = splitName[0];
		String lastName = splitName[splitName.length-1];

		for (int i = 1; i < splitName.length-1; i++)
		    givenName += splitName[i];

		try{
			String sql=" select playerid, givenname, lastname, debut, finalGame from players where upper( givenname) like ? or upper(lastname) like ?;";
			PreparedStatement statement =connection.prepareStatement(sql);
	                statement.setString(1, "%"+givenName+"%");
			statement.setString(2,"%"+lastName+"%");

			resultSet=statement.executeQuery();
                	while (resultSet.next()) {
				System.out.println(resultSet.getString("playerID")+", "+resultSet.getString("givenName")+" " + resultSet.getString("lastName")+", "+resultSet.getString("debut")+", "+resultSet.getString("finalGame"));
                }
			resultSet.close();
			statement.close();
		}catch (SQLException e)
		{
			e.printStackTrace(System.out);
        	}


    }

//Print out number of schools each player attended. Print out player firstname, lastname and total number of school that player attended


    public void numSchoolsAttended()
    {
	        ResultSet resultSet;
		try{
			String sql="  with colleges AS(    select players.playerID, players.firstname, lastname, collegePlaying.schoolID    from players inner join collegePlaying on players.playerid=collegePlaying.playerid       group by players.playerid,  players.firstname, lastname, collegePlaying.schoolID     )     select colleges.firstname, lastname, count(colleges.schoolID) as NumberSchoolsAttended      from colleges      group by playerid,  firstname, lastname    having count(schoolID)>1;";
			PreparedStatement statement =connection.prepareStatement(sql);
			resultSet=statement.executeQuery();
                while (resultSet.next()) {
				System.out.println(resultSet.getString("firstname")+" "+resultSet.getString("lastname")+", " +resultSet.getInt("NumberSchoolsAttended"));
                }
			resultSet.close();
			statement.close();
		}catch (SQLException e)
		{
			e.printStackTrace(System.out);
       		}
    }




/////////////////////////////////////////////////////////////////////

// which players has the highest number of games played. report  top 10 players firstname, lastname and their number of games played 
    public void top10ActivePlayers()
    {
       		ResultSet resultSet;
		try{
			String sql="select TOP 10 firstname, lastname, sum(appearances.G_all) as NumGames, max(salaries.salary) as highestSalary  from appearances inner join players  on   players.playerid=appearances.playerid  inner join salaries  on  players.playerID=salaries.playerID   and salaries.yearid=appearances.yearid    group by appearances.playerid, firstname, lastname   order by NUmGames DESC;";
				PreparedStatement statement =connection.prepareStatement(sql);
				resultSet=statement.executeQuery();
                while (resultSet.next()) {
				    System.out.println(resultSet.getString("firstname")+", "+resultSet.getString("lastname")+", " +resultSet.getInt("NumGames")+ ", " + resultSet.getInt("HighestSalary"));
                }
				resultSet.close();
				statement.close();
		}catch (SQLException e)
		{
			e.printStackTrace(System.out);
       		}
    }
/////////////////////////////////////////////////////////////////////


/////////////////////////////////////////////////////////////////////
    //Search for players who were born and studied in different countries. Print out each player firstname, last name, their birth country and the school they attended that is different from where they were born, and how many countries they studied that different from their birth country.

    public void multiNationPlayers()
    {
       		ResultSet resultSet;
		try{
			String sql="with t AS( select   players.playerid,players.firstname, lastname, birthCountry, schools.country as StudyCountry  from players    inner join collegePlaying     on collegePlaying.playerid=players.playerid     inner join schools   on schools.schoolid=collegePlaying.schoolid       where not players.birthCountry =schools.country    group by players.playerid, players.firstname, lastname, birthcountry, schools.Country      )    select firstname, lastname, birthcountry, studyCountry,count(studyCountry) as NumDifferCountries  from t  where not t.birthCountry =studyCountry    group by playerid,firstname, lastname, birthcountry, studyCountry order by playerid;";
			PreparedStatement statement =connection.prepareStatement(sql);
			resultSet=statement.executeQuery();
               		 while (resultSet.next()) {
				System.out.println(resultSet.getString("firstname")+", "+resultSet.getString("lastname")+", " +resultSet.getString("birthCountry")+", "+resultSet.getString("studyCountry")+", "+resultSet.getString("NumDifferCountries")) ;
                }
			resultSet.close();
			statement.close();
		}catch (SQLException e)
		{
			e.printStackTrace(System.out);
       		 }
    }
/////////////////////////////////////////////////////////////////////




}
                 
