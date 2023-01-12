use cs3380;

-- clean up
drop table if exists collegePlaying;
drop table if exists pitching;
drop table if exists appearances;
drop table if exists fielding;
drop table if exists batting;

drop table if exists salaries;
drop table if exists teamFranchises;
drop table if exists players;
drop table if exists teams;
drop table if exists schools;

CREATE TABLE players (
    playerID	varchar(20),

    birthYear INTEGER,
    birthMonth INTEGER,
    birthDay INTEGER,
    birthCountry VARCHAR(200),

    birthState VARCHAR(200),
    birthCity VARCHAR(200),
 

    deathYear INTEGER,
    deathMonth INTEGER,
    deathDay INTEGER,
    deathCountry VARCHAR(200),
	deathState VARCHAR(200),
    deathCity VARCHAR(200),
 

   	firstname	VARCHAR(200),
    lastname	VARCHAR(200),
    givenname	VARCHAR(200),

    weight FLOAT,
    height FLOAT,
    bats VARCHAR(20),
    throw  VARCHAR(20),
    debut  VARCHAR(20),
    finalGame  VARCHAR(20),
	PRIMARY KEY(playerID),
);


CREATE TABLE teams (

    yearID	INTEGER,
    lgID	varchar(20),
    teamId	varchar(20),

    franchID  VARCHAR(20),
    divID	 VARCHAR(20),
    Rank	INTEGER,
    G	INTEGER,
    Ghome	INTEGER,

    W	INTEGER,
    L	INTEGER,
    DivWin	 VARCHAR(20),
    WcWin	 VARCHAR(20),
    LgWIn	 VARCHAR(20),
    WSWin	 VARCHAR(20),

    R	INTEGER,
    AB	INTEGER,
    H	INTEGER,
    B2	INTEGER,
    B3	INTEGER,

    HR	INTEGER,
    BB	INTEGER,
    SO	INTEGER,
    SB	INTEGER,
    CS	INTEGER,

    HBP	INTEGER,
    SF	INTEGER,
    RA	INTEGER,
    ER	INTEGER,
    ERA	float,
    CG	INTEGER,

    SHO	INTEGER,
    SV	INTEGER,
    IPouts	INTEGER,
    HA	INTEGER,
    HRA	INTEGER,
    BBA	INTEGER,

    SOA	INTEGER,
    E	INTEGER,
    DP	INTEGER,
    FP	float,--NUMERIC,

    teamName	VARCHAR(200),---------------------------its name is name in excel
    attendance	INTEGER,
    BPF	INTEGER,

    PPF	INTEGER,
 --   teammIDBR VARCHAR(20),
  --  teamIDlahman45  VARCHAR(20),
--	teamIDretro VARCHAR(20),
    

	PRIMARY KEY(teamID, yearID, lgID)
);



CREATE TABLE teamFranchises (
	franchID	varchar(20),
	franchName	 VARCHAR(200),
	active varchar(20),
    NAassoc VARCHAR(20),
	PRIMARY KEY(franchID)
);

CREATE TABLE schools (
	schoolID	varchar(20),
	schoolname	VARCHAR(200),
	city VARCHAR(200),
    state VARCHAR(200),
    country VARCHAR(200),
	PRIMARY KEY(schoolID)
);



CREATE TABLE collegePlaying (
	playerID varchar(20),
	schoolID varchar(20),
	yearid	INTEGER,---- year id does not reference from anywhere ?


	PRIMARY KEY(yearID,playerID, schoolID),
	FOREIGN KEY(playerID) REFERENCES players(playerID),
	FOREIGN KEY(schoolID) REFERENCES schools(schoolID)
);

CREATE TABLE salaries (
	yearID INTEGER,
	teamID 	varchar(20),
	lgID varchar(20),
	playerID 	varchar(20),
	salary	INTEGER,

	FOREIGN KEY(playerID) REFERENCES players(playerID),
	FOREIGN KEY(teamID, yearID, lgID) REFERENCES teams(teamID, yearID, lgID),
	PRIMARY KEY(playerID,teamID, yearID, lgID)
);


CREATE TABLE pitching (

	playerID varchar(20),
    yearID INTEGER,

	stint	INTEGER,

    teamID varchar(20),
    lgID varchar(20),

	W	INTEGER,
	L	INTEGER,
	G	INTEGER,
	GS	INTEGER,

    CG	INTEGER,
	SHO	INTEGER,
	SV	INTEGER,
	IPouts	INTEGER,

	H	INTEGER,
	ER	INTEGER,
	HR	INTEGER,
	BB	INTEGER,

    SO	INTEGER,
	BAOpp	float,
	ERA	float,
	IBB	INTEGER,

    WP	INTEGER,
	HBP	INTEGER,
	BK	INTEGER,
	BFP	INTEGER,

    GF	INTEGER,
	R	INTEGER,

	FOREIGN KEY(playerID) REFERENCES players(playerID),
	FOREIGN KEY(teamID, yearID, lgID) REFERENCES teams(teamID, yearID, lgID),
	PRIMARY KEY(playerID,teamID, yearID, lgID, stint)
);


CREATE TABLE appearances (

	yearID INTEGER,
    teamID varchar(20),
    lgID varchar(20),
	playerID varchar(20),


	G_all	INTEGER,
	GS	INTEGER,
	G_batting	INTEGER,
	G_defense	INTEGER,
	G_p	INTEGER,

    G_c	INTEGER,
	G_1b	INTEGER,
	G_2b	INTEGER,
	G_3b	INTEGER,

	G_ss	INTEGER,
	G_lf	INTEGER,
	G_cf	INTEGER,
	G_rf	INTEGER,
	G_of	INTEGER,
	G_dh	INTEGER,

    G_ph	INTEGER,
	G_pr	INTEGER,

	FOREIGN KEY(playerID) REFERENCES players(playerID),
	FOREIGN KEY(teamID, yearID, lgID) REFERENCES teams(teamID, yearID, lgID),
	PRIMARY KEY(playerID,teamID, yearID, lgID)
);





CREATE TABLE fielding (


    playerID varchar(20),

    yearID INTEGER,

	stint	INTEGER,

	teamID varchar(20),
    lgID varchar(20),


	POS	VARCHAR(20),
	G	INTEGER,
	GS	INTEGER,
	InnOuts	INTEGER,

    PO	INTEGER,
	A	INTEGER,
	E	INTEGER,
	DP	INTEGER,

	FOREIGN KEY(playerID) REFERENCES players(playerID),
	FOREIGN KEY(teamID, yearID, lgID) REFERENCES teams(teamID, yearID, lgID),
	PRIMARY KEY(playerID,teamID, yearID, lgID, stint, POS)
);


CREATE TABLE batting (
    playerID varchar(20),

    yearID INTEGER,

	stint	INTEGER,
	
    teamID varchar(20),
    lgID varchar(20),

	G	INTEGER,
	AB	INTEGER,
	R	INTEGER,

    H	INTEGER,
	B2	INTEGER,
	B3	INTEGER,
	HR	INTEGER,

	RBI	INTEGER,
	SB	INTEGER,
	CS	INTEGER,
	BB	INTEGER,
	SO	INTEGER,
	

	IBB	INTEGER,
	HBP	INTEGER,
	SH	INTEGER,
	SF	INTEGER,
	GIDP	INTEGER,



	FOREIGN KEY(playerID) REFERENCES players(playerID),
	FOREIGN KEY(teamID, yearID, lgID) REFERENCES teams(teamID, yearID, lgID),
	PRIMARY KEY(playerID,teamID, yearID, lgID, stint)
);



