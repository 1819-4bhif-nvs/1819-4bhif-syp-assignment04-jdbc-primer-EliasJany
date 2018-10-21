package at.htl.ssv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

//Ssv = Spielesammlungsverwaltung
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SsvTest {
    public static final String Driver_String = "org.apache.jdbc.ClientDriver";
    public static final String Connection_String = "jdbc:derby//localhost:1527/db;create=true";
    public static final String user = "app";
    public static final String passwort = "app";
    public static Connection conn;

    @BeforeClass
    public static  void InitJdbc()
    {
        //Verbindung zur DB
        try{
            Class.forName(Driver_String);
            conn = DriverManager.getConnection(Connection_String,user,passwort);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n" + e.getMessage()+"\n");
            System.exit(1);
        }

        //Erstellen der Tabellen
        try {
            Statement stmt = conn.createStatement();
            String cmd = "CREATE TABLE entwickler(name varchar(255) constraint ent_pk PRIMARY KEY)";
            stmt.execute(cmd);

            cmd = "CREATE TABLE publisher(name varchar(255) constraint pub_pk PRIMARY KEY)";
            stmt.execute(cmd);

            cmd = "CREATE TABLE spiele("+
                    "name varchar(255) constraint game_pk PRIMARY KEY,"+
                    "entwickler varchar(255) not null,"+
                    "publisher varchar(255) not null,"+
                    "beschreibung varchar(255)"+
                    "veroeffentlichung int not null,"+
                    "\"Groesse(in GB)\" int not null,"+
                    "constraint ent_fk FOREIGN KEY (entwickler) references entwickler(name),"+
                    "constraint pub_fk FOREIGN KEY (publisher) references publisher(name))";
            stmt.execute(cmd);
            conn.commit();
        }
        catch (SQLException e)
        {
            System.err.println(e.getMessage()+"\n");
        }
    }

    @AfterClass
    public static void teardownJdbc(){
        //Tabellen löschen
        try{
            conn.createStatement().execute("drop table spiele");
            conn.createStatement().execute("drop table publisher");
            conn.createStatement().execute("drop table entwickler");
        }
        catch (SQLException e){
            System.out.println("Tabelle konnte nicht gelöscht werden:\n" + e.getMessage() + "\n");
        }


        //Connection schließen
        try {
            if(conn != null || !conn.isClosed()){
                conn.close();
                System.out.println("Good Bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void T01_InsertDataEntwickler(){
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String cmd = "INSERT INTO entwickler (name) values('Gearbox Software')";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO entwickler (name) values('AQURIA')";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO entwickler (name) values('DIMPS')";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO entwickler (name) values('Ubisoft')";
            countInserts += stmt.executeUpdate(cmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(4));
    }

    @Test
    public void T02_InsertDataPublisher(){
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String cmd = "INSERT INTO publisher (name) values('K2')";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO publisher (name) values('BANDAI NAMCO Entertaiment')";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO publisher (name) values('Ubisoft')";
            countInserts += stmt.executeUpdate(cmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(3));
    }


    @Test
    public void T03_InsertDataSpiele(){
        int countInserts = 0;

        try {
            Statement stmt = conn.createStatement();
            String cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Borderlands','Gearbox Software','K2','',2009,8)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Borderlands 2','Gearbox Software','K2','',2012,13)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Borderlands: The Pre-Sequel','Gearbox Software','K2','',2014,13)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Sword Art Online Re: Hollow Fragment','AQURIA','BANDAI NAMCO Entertaiment','',2018,30)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Sword Art Online: Hollow Realization','AQURIA','BANDAI NAMCO Entertaiment','',2017,40)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Sword Art Online: Fatal Bullet','DIMPS','BANDAI NAMCO Entertaiment','',2018,30)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Watch Dogs','Ubisoft','Ubisoft','',2014,25)";
            countInserts += stmt.executeUpdate(cmd);

            cmd = "INSERT INTO spiele (name,entwickler,publisher,beschreibung,veroeffentlichung,Groesse(in GB)) values('Watch Dogs 2','Ubisoft','Ubisoft','',2016,27)";
            countInserts += stmt.executeUpdate(cmd);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertThat(countInserts, is(8));
    }


    @Test
    public void T04_CheckDataEntwickler(){
        PreparedStatement prstmt = null;

        try {
            prstmt = conn.prepareStatement("SELECT name FROM entwickler");
            ResultSet rs = prstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Gearbox Software"));
            rs.next();
            assertThat(rs.getString("name"), is("AQURIA"));
            rs.next();
            assertThat(rs.getString("name"), is("DIMPS"));
            rs.next();
            assertThat(rs.getString("name"), is("Ubisoft"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void T05_CheckDataPublisher(){
        PreparedStatement prstmt = null;

        try {
            prstmt = conn.prepareStatement("SELECT name FROM publisher");
            ResultSet rs = prstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("K2"));
            rs.next();
            assertThat(rs.getString("name"), is("BANDAI NAMCO Entertaiment"));
            rs.next();
            assertThat(rs.getString("name"), is("Ubisoft"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void T06_CheckDataSpiele(){
        PreparedStatement prstmt = null;

        try {
            prstmt = conn.prepareStatement("SELECT name FROM spiele");
            ResultSet rs = prstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("name"), is("Borderlands"));
            rs.next();
            assertThat(rs.getString("name"), is("Borderlands 2"));
            rs.next();
            assertThat(rs.getString("name"), is("Borderlands: The Pre-Sequel"));
            rs.next();
            assertThat(rs.getString("name"), is("Sword Art Online Re: Hollow Fragment"));
            rs.next();
            assertThat(rs.getString("name"), is("Sword Art Online: Hollow Realization"));
            rs.next();
            assertThat(rs.getString("name"), is("Sword Art Online: Fatal Bullet"));
            rs.next();
            assertThat(rs.getString("name"), is("Watch Dogs"));
            rs.next();
            assertThat(rs.getString("name"), is("Watch Dogs 2"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
