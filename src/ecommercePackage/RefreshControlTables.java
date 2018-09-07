/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ecommercePackage;

import static ecommercePackage.UploadCustomer.log;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author travis
 */
public class RefreshControlTables {
    
    
    

   

    int lastProdQty = 0;

    protected static final Logger log = Logger.getLogger(Main.class.getName( ));
    Appender myAppender;
    SimpleLayout myLayout;

    
    
    SimpleDateFormat sdfYmdHms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    
    String mysqlUrl = "jdbc:mysql://localhost:3306/ecommerce";
    String mysqlDriver = "com.mysql.jdbc.Driver";
    String mysqlUserName = "ecommerce";
    String mysqlPassword = "ecommerce_connect";
    Connection mysqlConn = null;
    Statement mysqlStmt = null;
    ResultSet mysqlRs = null;


        
    String as400Url = "jdbc:as400://10.2.2.13/BATCHF"; // LIBRARY NAME
    String as400Driver = "com.ibm.as400.access.AS400JDBCDriver";
    String as400UserName = "QPGMR";
    String as400Password = "PGMR";
    Connection as400Conn = null;

    public RefreshControlTables(){
        /* Set logger priority level programmatically. Though this is
           better done externally  */

        log.setLevel(Level.INFO);
        /* Instantiate a layout and an appender, assign layout to
           appender programmatically */
        myLayout = new SimpleLayout();
        //myAppender = new ConsoleAppender(myLayout);    // Appender is
        ///* Assign appender to the logger programmatically */
        log.addAppender(myAppender);

        
        
    }
    
    
    
    
    public void refreshProducts() throws Throwable  {

        PropertyConfigurator.configure("logger.properties");
        log.info("Starting process.");


        
        // has it changed today? Go ahead and refresh it again. This may happen several times a day.
        Calendar updated = Calendar.getInstance();
        try {
                Class.forName(mysqlDriver).newInstance();
                mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                
                String mysqlSQL = "SELECT UPDATE_TIME " + 
                    "FROM information_schema.tables " + 
                    "WHERE  TABLE_SCHEMA = 'ecommerce' " + 
                    "AND TABLE_NAME = 'products';";
                PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                mysqlRs = statement.executeQuery();
                mysqlRs = statement.getResultSet();
                while (mysqlRs.next()) {
                        log.info("------------------------------------------------------------");

                        //29
                        String updatedString = mysqlRs.getString(1);
                        updated.setTime(sdfYmdHms.parse(updatedString));
                        String updatedInStringFormat = sdfYmdHms.format(updated.getTime());
                        log.info("products last updated on: " + updatedInStringFormat);
                }
                statement.close();
                mysqlConn.close();
        } catch (Exception e){
            e.printStackTrace();
            log.error("error while gettting updated time: " + e);
            throw(e);
        }        
            
        
        
       
        Calendar yesterdayAtThisTime = Calendar.getInstance();
        yesterdayAtThisTime.add(Calendar.DATE,-1);
        String yesterdayAtThisTimeInStringFormat = sdfYmdHms.format(yesterdayAtThisTime.getTime());
        log.info("yesterday: " + yesterdayAtThisTimeInStringFormat);
        
        
        
        if (updated.compareTo(yesterdayAtThisTime) == 1){
            log.info("products has been updated in the last day. Now refreshing table on as400.");
             // now if updated time is today, insert/update/delete accounts.
        
            try{
                    Class.forName(as400Driver).newInstance();
                    as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                    //20
                    String as400SQL = "DELETE FROM PRODUCTS";
                    PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);
                    as400Statement.execute();
                    as400Statement.close();
                    log.info("PRODUCTS TABLE TRUNCATED ON AS400.");
            } catch(Exception e){
                e.printStackTrace();
                log.error("ERROR WHILE INSERTING INTO AS400 TRANH: " + e);
                throw(e);
            }

            
            
            
            


            try{
                    Class.forName(mysqlDriver).newInstance();
                    mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                    String mysqlSQL = "SELECT * FROM products where status = 1";

                    PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                    mysqlRs = statement.executeQuery();
                    mysqlRs = statement.getResultSet();
                    while (mysqlRs.next()) {
                        String ACCT = mysqlRs.getString("acct");
                        String EVENT = mysqlRs.getString("event_code");
                        String CODE = mysqlRs.getString("code");
                        String DESC = mysqlRs.getString("description");
                        Double PRICE = mysqlRs.getDouble("price");
                        Double VALUE = mysqlRs.getDouble("value");
                        
                        
                           
                        // insert into ACCOUNTS
                            try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into PRODUCTS " +
                                        "(ACCT, EVENT, CODE, DESC, PRICE, VALUE)" +
                                        " VALUES(?,?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setString(1, ACCT);
                                as400Statement.setString(2, EVENT);
                                as400Statement.setString(3, CODE);
                                if (DESC.length() > 44){
                                    DESC = DESC.substring(0,44);
                                }
                                as400Statement.setString(4, DESC);
                                as400Statement.setDouble(5, PRICE);
                                as400Statement.setDouble(6, VALUE);
                                
                                as400Statement.execute();
                                as400Statement.close();
                                log.info("product code: " + ACCT + ", " + EVENT + "," + CODE + " added to PRODUCTS");
                            } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 PRODUCTS: " + e);
                                throw(e);
                            }

                    }
                    statement.close();
                    mysqlConn.close();
                } catch (Exception e){
                    e.printStackTrace();
                    log.error("ERROR WHILE SELECTING FROM mysql table: products. Error: " + e);
                    throw(e);
                }

            
            
        } else{
            log.info("Don't need to update products.");
        }
        
       


    }

    
    
    public void refreshBarcodedSeasonPasses() throws Throwable  {

        PropertyConfigurator.configure("logger.properties");
        log.info("Starting process.");
        
        // has it changed today? Go ahead and refresh it again. This may happen several times a day.
        Calendar updated = Calendar.getInstance();
        try {
                Class.forName(mysqlDriver).newInstance();
                mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                
                String mysqlSQL = "SELECT UPDATE_TIME " + 
                    "FROM information_schema.tables " + 
                    "WHERE  TABLE_SCHEMA = 'ecommerce' " + 
                    "AND TABLE_NAME = 'barcoded_season_passes';";
                PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                mysqlRs = statement.executeQuery();
                mysqlRs = statement.getResultSet();
                while (mysqlRs.next()) {
                        log.info("------------------------------------------------------------");

                        //29
                        String updatedString = mysqlRs.getString(1);
                        updated.setTime(sdfYmdHms.parse(updatedString));
                        String updatedInStringFormat = sdfYmdHms.format(updated.getTime());
                        log.info("barcoded_season_passes last updated on: " + updatedInStringFormat);
                }
                statement.close();
                mysqlConn.close();
        } catch (Exception e){
            e.printStackTrace();
            log.error("error while gettting updated time: " + e);
            throw(e);
        }        
            
        
        
       
        Calendar yesterdayAtThisTime = Calendar.getInstance();
        yesterdayAtThisTime.add(Calendar.DATE,-1);
        String yesterdayAtThisTimeInStringFormat = sdfYmdHms.format(yesterdayAtThisTime.getTime());
        log.info("yesterday: " + yesterdayAtThisTimeInStringFormat);
        
        
        
        if (updated.compareTo(yesterdayAtThisTime) == 1){
            log.info("barcoded_season_passes has been updated in the last day. Now refreshing table on as400.");
             // now if updated time is today, insert/update/delete accounts.
        
            try{
                    Class.forName(as400Driver).newInstance();
                    as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                    //20
                    String as400SQL = "DELETE FROM SPASSES";
                    PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);
                    as400Statement.execute();
                    as400Statement.close();
                    log.info("SPASSES TABLE TRUNCATED ON AS400.");
            } catch(Exception e){
                e.printStackTrace();
                log.error("ERROR WHILE TRUNCATING SPASSES TABLE: " + e);
                throw(e);
            }

            
            
            
            


            try{
                    Class.forName(mysqlDriver).newInstance();
                    mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                    String mysqlSQL = "SELECT * FROM barcoded_season_passes WHERE shipping_method = 'F2S'";

                    PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                    mysqlRs = statement.executeQuery();
                    mysqlRs = statement.getResultSet();
                    while (mysqlRs.next()) {
                        int ID = mysqlRs.getInt("id");
                        String CONF = mysqlRs.getString("confirmation");
                        String SHPMTH = mysqlRs.getString("shipping_method");
                        String FIRST = mysqlRs.getString("first");
                        String LAST = mysqlRs.getString("last");
                        String BARC = mysqlRs.getString("barcode");
                        
                           
                        // insert into EVENTS
                            try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into SPASSES " +
                                        "(CONF, SHPMTH, FIRST, LAST, BARC)" +
                                        " VALUES(?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setString(1, CONF);
                                as400Statement.setString(2, SHPMTH);
                                as400Statement.setString(3, FIRST);
                                as400Statement.setString(4, LAST);
                                as400Statement.setString(5, BARC);
                                
                                as400Statement.execute();
                                as400Statement.close();
                                log.info("barcoded_season_pass: " + CONF + ", " + BARC + " added to as400.");
                            } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 SPASSES: " + e);
                                throw(e);
                            }

                    }
                    statement.close();
                    mysqlConn.close();
                } catch (Exception e){
                    e.printStackTrace();
                    log.error("ERROR WHILE SELECTING FROM mysql table: barcoded_season_passes. Error: " + e);
                    throw(e);
                }

            
            
        } else{
            log.info("Don't need to update SPASSES.");
        }
        
       


    }

    
    
    
    public void refreshEvents() throws Throwable  {

        PropertyConfigurator.configure("logger.properties");
        log.info("Starting process.");


        
        // has it changed today? Go ahead and refresh it again. This may happen several times a day.
        Calendar updated = Calendar.getInstance();
        try {
                Class.forName(mysqlDriver).newInstance();
                mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                
                String mysqlSQL = "SELECT UPDATE_TIME " + 
                    "FROM information_schema.tables " + 
                    "WHERE  TABLE_SCHEMA = 'ecommerce' " + 
                    "AND TABLE_NAME = 'events';";
                PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                mysqlRs = statement.executeQuery();
                mysqlRs = statement.getResultSet();
                while (mysqlRs.next()) {
                        log.info("------------------------------------------------------------");

                        //29
                        String updatedString = mysqlRs.getString(1);
                        updated.setTime(sdfYmdHms.parse(updatedString));
                        String updatedInStringFormat = sdfYmdHms.format(updated.getTime());
                        log.info("events last updated on: " + updatedInStringFormat);
                }
                statement.close();
                mysqlConn.close();
        } catch (Exception e){
            e.printStackTrace();
            log.error("error while gettting updated time: " + e);
            throw(e);
        }        
            
        
        
       
        Calendar yesterdayAtThisTime = Calendar.getInstance();
        yesterdayAtThisTime.add(Calendar.DATE,-1);
        String yesterdayAtThisTimeInStringFormat = sdfYmdHms.format(yesterdayAtThisTime.getTime());
        log.info("yesterday: " + yesterdayAtThisTimeInStringFormat);
        
        
        
        if (updated.compareTo(yesterdayAtThisTime) == 1){
            log.info("Events has been updated in the last day. Now refreshing table on as400.");
             // now if updated time is today, insert/update/delete accounts.
        
            try{
                    Class.forName(as400Driver).newInstance();
                    as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                    //20
                    String as400SQL = "DELETE FROM EVENTS";
                    PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);
                    as400Statement.execute();
                    as400Statement.close();
                    log.info("EVENTS TABLE TRUNCATED ON AS400.");
            } catch(Exception e){
                e.printStackTrace();
                log.error("ERROR WHILE TRUNCATING EVENTS TABLE: " + e);
                throw(e);
            }

            
            
            
            


            try{
                    Class.forName(mysqlDriver).newInstance();
                    mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                    String mysqlSQL = "SELECT * FROM events where status = 1";

                    PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                    mysqlRs = statement.executeQuery();
                    mysqlRs = statement.getResultSet();
                    while (mysqlRs.next()) {
                        int ID = mysqlRs.getInt("id");
                        String ACCT = mysqlRs.getString("acct");
                        String CODE = mysqlRs.getString("code");
                        String DESC = mysqlRs.getString("description");
                        String TITLE = mysqlRs.getString("title");
                        int STATUS = mysqlRs.getInt("status");
                        
                           
                        // insert into EVENTS
                            try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into EVENTS " +
                                        "(ACCT, CODE, DESC, TITLE, STATUS)" +
                                        " VALUES(?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setString(1, ACCT);
                                as400Statement.setString(2, CODE);
                                as400Statement.setString(3, DESC);
                                as400Statement.setString(4, TITLE);
                                as400Statement.setInt(5, STATUS);
                                
                                as400Statement.execute();
                                as400Statement.close();
                                log.info("EVENT: " + CODE + " added to EVENTS");
                            } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 ACCOUNTS: " + e);
                                throw(e);
                            }

                    }
                    statement.close();
                    mysqlConn.close();
                } catch (Exception e){
                    e.printStackTrace();
                    log.error("ERROR WHILE SELECTING FROM mysql table: events. Error: " + e);
                    throw(e);
                }

            
            
        } else{
            log.info("Don't need to update events.");
        }
        
       


    }

    
    
    
    
    
    public void refreshAccounts() throws Throwable  {

        PropertyConfigurator.configure("logger.properties");
        log.info("Starting process.");


        
        // has it changed today? Go ahead and refresh it again. This may happen several times a day.
        Calendar updated = Calendar.getInstance();
        try {
                Class.forName(mysqlDriver).newInstance();
                mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                
                String mysqlSQL = "SELECT UPDATE_TIME " + 
                    "FROM information_schema.tables " + 
                    "WHERE  TABLE_SCHEMA = 'ecommerce' " + 
                    "AND TABLE_NAME = 'accounts';";
                PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                mysqlRs = statement.executeQuery();
                mysqlRs = statement.getResultSet();
                while (mysqlRs.next()) {
                        log.info("------------------------------------------------------------");

                        //29
                        String updatedString = mysqlRs.getString(1);
                        updated.setTime(sdfYmdHms.parse(updatedString));
                        String updatedInStringFormat = sdfYmdHms.format(updated.getTime());
                        log.info("accounts last updated on: " + updatedInStringFormat);
                }
                statement.close();
                mysqlConn.close();
        } catch (Exception e){
            e.printStackTrace();
            log.error("error while gettting updated time: " + e);
            throw(e);
        }        
            
        
        
       
        Calendar yesterdayAtThisTime = Calendar.getInstance();
        yesterdayAtThisTime.add(Calendar.DATE,-1);
        String yesterdayAtThisTimeInStringFormat = sdfYmdHms.format(yesterdayAtThisTime.getTime());
        log.info("yesterday: " + yesterdayAtThisTimeInStringFormat);
        
        
        
        if (updated.compareTo(yesterdayAtThisTime) == 1){
            log.info("Accounts has been updated in the last day. Now refreshing table on as400.");
             // now if updated time is today, insert/update/delete accounts.
        
            try{
                    Class.forName(as400Driver).newInstance();
                    as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                    //20
                    String as400SQL = "DELETE FROM ACCOUNTS";
                    PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);
                    as400Statement.execute();
                    as400Statement.close();
                    log.info("ACCOUNTS TABLE TRUNCATED ON AS400.");
            } catch(Exception e){
                e.printStackTrace();
                log.error("ERROR WHILE INSERTING INTO AS400 TRANH: " + e);
                throw(e);
            }

            
            
            
            


            try{
                    Class.forName(mysqlDriver).newInstance();
                    mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                    String mysqlSQL = "SELECT * FROM accounts where status = 1 AND use_direct_fulfillment = 1";

                    PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                    mysqlRs = statement.executeQuery();
                    mysqlRs = statement.getResultSet();
                    while (mysqlRs.next()) {
                        int ID = mysqlRs.getInt("id");
                        String ACCT = mysqlRs.getString("acct");
                        String DESC = mysqlRs.getString("description");
                        String CONTACT = mysqlRs.getString("contact");
                        String EMAIL = mysqlRs.getString("email");
                        String TELE = mysqlRs.getString("telephone");
                        String RADD1 = mysqlRs.getString("df_return_address_1");
                        String RADD2 = mysqlRs.getString("df_return_address_2");
                        String RADD3 = mysqlRs.getString("df_return_address_3");
                        String RADD4 = mysqlRs.getString("df_return_address_4");
                        String RADD5 = mysqlRs.getString("df_return_address_5");
                        String FSHPNM = mysqlRs.getString("df_fedex_shipper_number");
                        String USHPNM = mysqlRs.getString("df_ups_shipper_number");

                           
                        // insert into ACCOUNTS
                            try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into ACCOUNTS " +
                                        "(ID,ACCT,DESC,CONTACT,TELE,EMAIL,RADD1,RADD2,RADD3,RADD4,RADD5,FSHPNM,USHPNM)" +
                                        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setInt(1, ID);
                                as400Statement.setString(2, ACCT);
                                as400Statement.setString(3, DESC);
                                as400Statement.setString(4, CONTACT);
                                as400Statement.setString(5, TELE);
                                as400Statement.setString(6, EMAIL);
                                as400Statement.setString(7, RADD1);
                                as400Statement.setString(8, RADD2);
                                as400Statement.setString(9, RADD3);
                                as400Statement.setString(10,RADD4);
                                as400Statement.setString(11,RADD5);
                                as400Statement.setString(12,FSHPNM);
                                as400Statement.setString(13,USHPNM);
                                
                                as400Statement.execute();
                                as400Statement.close();
                                log.info("ACCT: " + ACCT + " added to ACCOUNTS");
                            } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 ACCOUNTS: " + e);
                                throw(e);
                            }

                    }
                    statement.close();
                    mysqlConn.close();
                } catch (Exception e){
                    e.printStackTrace();
                    log.error("ERROR WHILE SELECTING FROM mysql table: accounts. Error: " + e);
                    throw(e);
                }

            
            
        } else{
            log.info("Don't need to update accounts.");
        }
        
       


    }

    
    
    
    
}
