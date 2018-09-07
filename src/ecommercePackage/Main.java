/*
DESCRIPTION: 
    jdbc example migrating data from AS400 (remote ip address) to MySQL. 
    transfer from AS400 to MySQL

RUNNING:
    Needs to be executed on linux machine hosting MySQL as shell script
        ideally from crontab scheduler.

TODO (2018-09-06): 
   * needs to be refactored to use objects 
   * needs hamcrest library added to project
   
 */



package ecommercePackage;


import java.sql.*;
import org.apache.log4j.*;
import org.apache.log4j.PropertyConfigurator;
import java.text.SimpleDateFormat;
import java.util.Date;








/**
 *
 * @author travis
 */
public class Main {




    
    protected static final Logger myLogger = Logger.getLogger(Main.class.getName( ));
    Appender myAppender;
    SimpleLayout myLayout;



     /* Constructor */
    public Main() {
        /* Set logger priority level programmatically. Though this is
           better done externally  */

        myLogger.setLevel(Level.INFO);
        /* Instantiate a layout and an appender, assign layout to
           appender programmatically */
        myLayout = new SimpleLayout();
        //myAppender = new ConsoleAppender(myLayout);    // Appender is
        ///* Assign appender to the logger programmatically */
        myLogger.addAppender(myAppender);

    } //end constructor


    public static void main(String[] args) throws Throwable  {

        updateOneCustomer("8736");

        updateOneCustomer("71656");


    }



    /**
     * @param args the command line arguments
     */
    public static void updateOneCustomer(String customerNumber) throws Throwable  {

        PropertyConfigurator.configure("logger.properties");
        myLogger.info("Starting process for customerNumber: " + customerNumber);

        String mysqlUrl = "jdbc:mysql://localhost:3306/ecommerce";
        String mysqlDriver = "com.mysql.jdbc.Driver";
        String mysqlUserName = "MySQL_username";
        String mysqlPassword = "MySQL_password";
        Connection mysqlConn = null;
        Statement mysqlStmt = null;
        ResultSet mysqlRs = null;

        String as400Url = "jdbc:as400://ipaddressofas400/physicalorlogicalfileonas400"; 
        String as400Driver = "com.ibm.as400.access.AS400JDBCDriver";
        String as400UserName = "AS400_username";
        String as400Password = "AS400_password";
        Connection as400Conn = null;
        Statement as400Statement = null;
        ResultSet as400Rs = null;
        ResultSet as400Rs2 = null;




        // read as400 - all with notified <> "Y".
         try{
                Class.forName(as400Driver).newInstance();
                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                //20
                String as400SQL = "SELECT CONF, TRACK, MYUSER, SHPON,SHPDES FROM BATCHF.TRANH WHERE CACCT = ? AND SHPON > ? AND NOTIFIED <> ?";
                PreparedStatement as400statement = as400Conn.prepareStatement(as400SQL);
                as400statement.setString(1, customerNumber);
                as400statement.setString(2, "20180000");
                as400statement.setString(3, "Y");
                as400Rs = as400statement.executeQuery();
                as400Rs = as400statement.getResultSet();
                int totalRecordsUpdated = 0;
                while (as400Rs.next()) {
                // for testing...for (int iii=0;iii<=1;iii++) {
                //as400Rs.next();
                        String CONF = as400Rs.getString(1).trim();
                        String WWLTRACK = as400Rs.getString(2).trim();
                        String WWLMYUSER = as400Rs.getString(3).trim();
                        String WWLSHPON = as400Rs.getString(4).trim();
                        String WWLSHPDES = as400Rs.getString(5).trim();
                        myLogger.info("---------------------------------------------------------------");
                        myLogger.info("CONF: " + CONF);
                        myLogger.info("WWLTRACK: " + WWLTRACK);
                        myLogger.info("WWLMYUSER: " + WWLMYUSER);
                        myLogger.info("WWLSHPON: " + WWLSHPON);
                        myLogger.info("WWLSHPDES: " + WWLSHPDES);


                        
                        String assignedHash = "";
                        
                        // get numbers assigned.
                        String as400SQL2 = "SELECT PCODE,ASSIGNED FROM BATCHF.TRANR WHERE CACCT = ? AND CONF = ?";
                        PreparedStatement as400statement2 = as400Conn.prepareStatement(as400SQL2);
                        as400statement2.setString(1, customerNumber);
                        as400statement2.setString(2, CONF);
                        as400Rs2 = as400statement2.executeQuery();
                        as400Rs2 = as400statement2.getResultSet();
                        while (as400Rs2.next()) {
                             assignedHash += as400Rs2.getString(1).trim() + "~" + as400Rs2.getString(2).trim() + "|";
                        }
                        as400Rs2.close();
                        myLogger.info("WWL_NUMBERS_ASSIGNED: " + assignedHash);
                        


                        try{
                                        Class.forName(mysqlDriver).newInstance();
                                        mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                                        //20
                                        String mysqlSQL = "update TRANSACTIONS SET WWLTRACK = ?, WWLMYUSER = ?, WWLSHPON = ?, WWL_NUMBERS_ASSIGNED = ?, SHPDES = ? WHERE CACCT = ? AND CONF = ?";
                                        PreparedStatement mysqlStatement = mysqlConn.prepareStatement(mysqlSQL);


                                        mysqlStatement.setString(1, WWLTRACK);
                                        mysqlStatement.setString(2, WWLMYUSER);
                                        mysqlStatement.setString(3, WWLSHPON);
                                        mysqlStatement.setString(4, assignedHash);
                                        mysqlStatement.setString(5, WWLSHPDES);
                                        mysqlStatement.setString(6, customerNumber);
                                        mysqlStatement.setString(7, CONF);

                                        mysqlStatement.execute();
                                        mysqlStatement.close();
                                        totalRecordsUpdated++;
                                        myLogger.info("Updated mysql TRANSACTIONS info.");

                         } catch(Exception e){
                                e.printStackTrace();
                                myLogger.error("ERROR WHILE UPDATING TRANSACTIONS in mysql database: " + e);
                                throw(e);
                         } 
                         


                        // NOW UPDATE AS400 NOTIFIED FIELD.
                        try{
                                        //Class.forName(as400Driver).newInstance();
                                        //as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                        //20
                                        String as400SQL3 = "update BATCHF.TRANH SET NOTIFIED = ? WHERE CACCT = ? AND CONF = ?";
                                        PreparedStatement as400Statement3 = as400Conn.prepareStatement(as400SQL3);


                                        as400Statement3.setString(1, "Y");
                                        as400Statement3.setString(2, customerNumber);
                                        as400Statement3.setString(3, CONF);

                                        as400Statement3.execute();
                                        as400Statement3.close();
                                        myLogger.info("Updated Notified on as400.");

                         } catch(Exception e){
                                e.printStackTrace();
                                myLogger.error("ERROR WHILE UPDATING TRANH NOTIFIED FIELD: " + e);
                                throw(e);
                         }

                         
                        




                }
                as400statement.close();
                as400Conn.close();
                myLogger.info("==============================================");
                myLogger.info("Total records Updated: " + totalRecordsUpdated);

         } catch(Exception e){
                e.printStackTrace();
                myLogger.error("ERROR WHILE READING FROM AS400 TRANH FILE: " + e);
                throw(e);
         }

        // For each order... update WWLTRACK, WWLMYUSER, WWLSHPON, WWL_NUMBERS_ASSIGNED

















        






         // now refresh INV_SUM
         // read as400 - all with notified <> "Y".
        // tuncate INV_SUM
        try{
                        Class.forName(mysqlDriver).newInstance();
                        mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                        //20
                        String mysqlSQL = "delete from INV_SUM where CACCT = '" + customerNumber + "'";
                        PreparedStatement mysqlStatement = mysqlConn.prepareStatement(mysqlSQL);
                        mysqlStatement.execute();
                        mysqlStatement.close();
                        myLogger.info("INV_SUM records deleted.");
         } catch(Exception e){
                e.printStackTrace();
                myLogger.error("ERROR WHILE TRUNCATING INV_SUM: " + e);
                throw(e);
         }




        try{
                Class.forName(as400Driver).newInstance();
                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                //20
                String as400SQL = "SELECT COUNT(PCODE),PCODE,PDESC FROM BATCHF.INV WHERE CACCT = ? GROUP BY PCODE, PDESC";
                PreparedStatement statement = as400Conn.prepareStatement(as400SQL);
                statement.setString(1, customerNumber);
                as400Rs = statement.executeQuery();
                as400Rs = statement.getResultSet();
                while (as400Rs.next()) {
                    int PQTY = as400Rs.getInt(1);
                    String PCODE = as400Rs.getString(2);
                    String PDESC = as400Rs.getString(3);


                    try{
                                        Class.forName(mysqlDriver).newInstance();
                                        as400Conn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                                        //20
                                        String mysqlSQL = "insert into INV_SUM (PCODE,PDESC,PQTY,CACCT) values(?,?,?,?)";
                                        PreparedStatement mysqlStatement = mysqlConn.prepareStatement(mysqlSQL);


                                        mysqlStatement.setString(1, PCODE);
                                        mysqlStatement.setString(2, PDESC);
                                        mysqlStatement.setInt(3, PQTY);
                                        mysqlStatement.setString(4, customerNumber);
                                        mysqlStatement.execute();
                                        mysqlStatement.close();
                                        myLogger.info("PCODE: " + PCODE + " inserted into INV_SUM.");

                         } catch(Exception e){
                                e.printStackTrace();
                                myLogger.error("ERROR WHILE INSERTING INTO INV_SUM in mysql database: " + e);
                                throw(e);
                         }

                }

           } catch(Exception e){
                e.printStackTrace();
                myLogger.error("ERROR WHILE READING INV: " + e);
                throw(e);
         }








        
        

         myLogger.info("Sending emails...");
         // SEND ALL EMAILS
           try{
                //  http://www.fluffycat.com/Java/PreparedStatement/
                Class.forName(mysqlDriver).newInstance();
                mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);


                String mysqlSQL = "SELECT " +
                        "CONF,SBUS,SFIRST,SLAST,SADD1,SADD2,SCITY,SST,SZIP,SCNTRY,BBUS," +
                        "BFIRST,BLAST,BADD1,BADD2,BCITY,BST,BZIP,BCNTRY,EMAIL,DPHONE,EPHONE,SHPMTH," +
                        "SHPDES,SCOST, WTOTAL,RCVON,PRODUCTS,SUBTOTAL,WWLSHPON,WWLTRACK " +
                        "FROM TRANSACTIONS WHERE CACCT = ? AND WWLSHPON > ? AND WWL_EMAIL_SENT <> ?";
                PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                statement.setString(1, customerNumber);
                statement.setString(2, "");
                statement.setString(3, "Y");
                mysqlRs = statement.executeQuery();
                mysqlRs = statement.getResultSet();
                while (mysqlRs.next()) {




                        //29
                        String CONF = mysqlRs.getString(1);
                        String SBUS = mysqlRs.getString(2);
                        String SFIRST = mysqlRs.getString(3);
                        String SLAST = mysqlRs.getString(4);
                        String SADD1 = mysqlRs.getString(5);
                        String SADD2 = mysqlRs.getString(6);
                        String SCITY = mysqlRs.getString(7);
                        String SST = mysqlRs.getString(8);
                        String SZIP = mysqlRs.getString(9);
                        String SCNTRY = mysqlRs.getString(10);
                        String BBUS = mysqlRs.getString(11);
                        String BFIRST = mysqlRs.getString(12);
                        String BLAST = mysqlRs.getString(13);
                        String BADD1 = mysqlRs.getString(14);
                        String BADD2 = mysqlRs.getString(15);
                        String BCITY = mysqlRs.getString(16);
                        String BST = mysqlRs.getString(17);
                        String BZIP = mysqlRs.getString(18);
                        String BCNTRY = mysqlRs.getString(19);
                        String EMAIL = mysqlRs.getString(20);
                        String DPHONE = mysqlRs.getString(21);
                        String EPHONE = mysqlRs.getString(22);
                        String SHPMTH = mysqlRs.getString(23);
                        String SHPDES = mysqlRs.getString(24);
                        String SCOST = mysqlRs.getString(25);
                        Double WTOTAL = mysqlRs.getDouble(26);
                        String RCVON = mysqlRs.getString(27);
                        String PRODUCTS = mysqlRs.getString(28);
                        Double SUBTOTAL = mysqlRs.getDouble(29);
                        String WWLSHPON = mysqlRs.getString(30);
                        String WWLTRACK = mysqlRs.getString(31);


                        SimpleDateFormat sdfSource = new SimpleDateFormat("yyyyMMdd");
                        Date WWLSHPONDate = sdfSource.parse(WWLSHPON);

                        SimpleDateFormat sdf2 = new SimpleDateFormat("EEEE, MMMM d, yyyy");
                        String zdateshipped = sdf2.format(WWLSHPONDate);

                        


                        if (!SBUS.equals("")){SBUS += "<BR>";}
                        if (!SADD1.equals("")){SADD1 += "<BR>";}
                        if (!SADD2.equals("")){SADD2 += "<BR>";}


                       
                        

                        String trackMessage = "";

                        if (WWLTRACK.length() > 2){
                            if (SHPDES.toUpperCase().contains("FED")){
                              trackMessage = "Your FedEx tracking number is " + WWLTRACK + ".  For your convenience, click on the line below to access the tracking feature or contact FedEx at 1.800.GoFedEx<BR><BR>" +
                              "<a href=\"http://www.fedex.com/Tracking?ascend_header=1&clienttype=dotcom&cntry_code=us&language=english&tracknumbers=" + WWLTRACK + "\">" +
                              "<font color=\"blue\">http://www.fedex.com/Tracking?ascend_header=1&clienttype=dotcom&cntry_code=us&language=english&tracknumbers=" + WWLTRACK + "</FONT></A>" +
                              "<BR><BR>";
                            }
                            else if (SHPDES.toUpperCase().contains("POSTAL")){
                              trackMessage = "";
                            }

                        }

                        Date today = new Date();  
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");  
                        String year = dateFormat.format(today);  

                        
                        String emailFrom = "";
                        String emailReplyTo = "";
                        String emailSubject = "";
                        String emailHtmlData = "";
                        
                        // TODO: GET THIS FROM THE DATABASE
                        if (customerNumber.equals("71656")){
                            emailFrom = "State Fair of Virginia <statefairva@wwlnotify.com>";
                            emailReplyTo = "State Fair of Virginia <tickets@statefairva.org>";
                            emailSubject = "YOUR STATE FAIR ORDER HAS BEEN SHIPPED";
                            emailHtmlData = "" +
                            "<html><body bgcolor=#ffffff><br>" +
                            "<font face = \"arial\" color = \"#000000\">Dear " + BFIRST + ",<br><br>" +
                            "Your " + year + " State Fair of Virginia ticket order is being shipped to you via <font color=blue>" + SHPDES + "</font> shipping on <font color=blue>" + zdateshipped + "</font> to the following address:<BR><BR>";

                            
                            emailHtmlData += SBUS +
                            SFIRST + " " + SLAST + "<BR>" +
                            SADD1 +
                            SADD2 +
                            SCITY + " " + SST + "  " + SZIP + "<BR><BR>";

                            emailHtmlData += "" + trackMessage +
                            "<p><font face=\"Arial, Helvetica, sans-serif\" color=\"#000000\" size=\"3\">If you have questions about your order, please reach us"+
                                    " at <a href=\"mailto:tickets@statefairva.org\">tickets@statefairva.org</a> or by phone at (804) 994-2800." +
                                    "</font></p>"        +
                            "</font>" +
                            "</body></html>";
                            

                        } else if (customerNumber.equals("8736")){
                            emailFrom = "cwsofomaha@wwlinc.com";
                            emailReplyTo = "cwsofomaha@wwlinc.com";
                            emailSubject = "YOUR NCAA COLLEGE WORLD SERIES ORDER HAS BEEN SHIPPED";
                            emailHtmlData = "" +
                            "<html><body bgcolor=#ffffff><br>" +
                            "<font face = \"arial\" color = \"#000000\">Dear " + BFIRST + ",<br><br>" +
                            "Your " + year + " NCAA College World Series ticket order is being shipped to you via <font color=blue>" + SHPDES + "</font> shipping on <font color=blue>" + zdateshipped + "</font> to the following address:<BR><BR>";

                            
                            emailHtmlData += SBUS +
                            SFIRST + " " + SLAST + "<BR>" +
                            SADD1 +
                            SADD2 +
                            SCITY + " " + SST + "  " + SZIP + "<BR><BR>";

                            emailHtmlData += "" + trackMessage +
                            "<p><font face=\"Arial, Helvetica, sans-serif\" color=\"#000000\" size=\"3\">If you have questions about your order, please reach us"+
                                    " at <a href=\"mailto:carol@cwsomaha.com\">carol@cwsomaha.com</a> or by phone at (402) 554-4404." +
                                    "</font></p>"        +
                            "</font>" +
                            "</body></html>";
                        
                        }
                        
                        
                        
                        



                        //myLogger.info("Sending email for confirmation: " + CONF + " to " + EMAIL);

                        TravisMail thisemail = new TravisMail();

                        //emailFrom = "tporter@wwlinc.com";  // temporary.
                        myLogger.info("Sending email for confirmation: " + CONF + " to " + EMAIL);
                        String status = "";
                        status = thisemail.sendHTMLMail(emailFrom, emailReplyTo, EMAIL, emailSubject, emailHtmlData);
                        myLogger.info("status of normal email send: " + status);
                        // send a copy to me.
                        status = thisemail.sendHTMLMail(emailFrom, emailReplyTo, "travisporter@tkporter.com", emailSubject, emailHtmlData);
                        myLogger.info("status of copy send to Travis: " + status);




                         try{
                                String mysqlSQL2 = "update TRANSACTIONS SET WWL_EMAIL_SENT = ? WHERE CACCT = ? AND CONF = ?";
                                PreparedStatement mysqlStatement2 = mysqlConn.prepareStatement(mysqlSQL2);
                                mysqlStatement2.setString(1, "Y");
                                mysqlStatement2.setString(2, customerNumber);
                                mysqlStatement2.setString(3, CONF);
                                mysqlStatement2.execute();
                                mysqlStatement2.close();
                                myLogger.info("Updated EMAIL_SENT");

                         } catch(Exception e){
                                e.printStackTrace();
                                myLogger.error("ERROR WHILE UPDATING EMAIL_SENT: " + e);
                                throw(e);
                         }
                        





            }
                statement.close();
                mysqlConn.close();
            } catch (Exception e){
                e.printStackTrace();
                myLogger.error("ERROR WHILE SELECTING FROM TRANSACTIONS (FOR EMAIL NOTIFICATION): " + e);
                throw(e);
            }










    }

}
