/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ecommercePackage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
public class UploadCustomer {
    
    
    

   

    int lastProdQty = 0;

    protected static final Logger log = Logger.getLogger(Main.class.getName( ));
    Appender myAppender;
    SimpleLayout myLayout;

    
    
    String mysqlUrl = "jdbc:mysql://localhost:3306/ecommerce";
    String mysqlDriver = "com.mysql.jdbc.Driver";
    String mysqlUserName = "MySQL_username";
    String mysqlPassword = "MySQL_password";
    Connection mysqlConn = null;
    Statement mysqlStmt = null;
    ResultSet mysqlRs = null;


        
    String as400Url = "jdbc:as400://AS400ipaddress/physicalorlogicalfileonas400";
    String as400Driver = "com.ibm.as400.access.AS400JDBCDriver";
    String as400UserName = "AS400_username";
    String as400Password = "AS400_password";
    Connection as400Conn = null;

    public UploadCustomer(){
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
    
    
    
    public void uploadOneCustomer(String customerNumber) throws Throwable  {

        PropertyConfigurator.configure("logger.properties");
        log.info("Starting process.");


        // connect to mysql loop through all records over 3 hours old and with a transferred date = '0000000000'.
        Date dateNow = new Date();
        long timeThreeHoursAgo = dateNow.getTime(); // ////////// temporary
        Date dateThreeHoursAgo = new Date(timeThreeHoursAgo);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        StringBuilder threeHoursAgoStringBuilder = new StringBuilder( sdf.format( dateThreeHoursAgo ) );
        String threeHoursAgoString = threeHoursAgoStringBuilder.toString();
        log.info("Date three hours ago: " + threeHoursAgoString);

        log.info("-------------- customer Number: " + customerNumber + " -------------");    
        
       
        



        try{
                Class.forName(mysqlDriver).newInstance();
                mysqlConn = DriverManager.getConnection(mysqlUrl,mysqlUserName, mysqlPassword);
                String mysqlSQL = "SELECT " +
                        "CONF,event_code, SBUS,SFIRST,SLAST,SADD1,SADD2,SCITY,SST,SZIP,SCNTRY,BBUS," +
                        "BFIRST,BLAST,BADD1,BADD2,BCITY,BST,BZIP,BCNTRY,EMAIL,DPHONE,EPHONE,SHPMTH," +
                        "SHPDES,SCOST, WTOTAL,RCVON,PRODUCTS,SUBTOTAL " +
                        "FROM TRANSACTIONS WHERE CACCT = ? AND RCVON <= ? AND TRANSFERRED_DT = ? AND (SHPMTH <> 'WC') AND (SHPMTH <> 'PAH')";
                
                PreparedStatement statement = mysqlConn.prepareStatement(mysqlSQL);
                statement.setString(1, customerNumber);
                statement.setString(2, threeHoursAgoString);
                statement.setString(3, "00000000000000");
                mysqlRs = statement.executeQuery();
                mysqlRs = statement.getResultSet();
                while (mysqlRs.next()) {

                        log.info("------------------------------------------------------------");

                        //29
                        String CONF = mysqlRs.getString(1);
                        String eventCode = mysqlRs.getString(2);
                        String SBUS = mysqlRs.getString(3);
                        String SFIRST = mysqlRs.getString(4);
                        String SLAST = mysqlRs.getString(5);
                        String SADD1 = mysqlRs.getString(6);
                        String SADD2 = mysqlRs.getString(7);
                        String SCITY = mysqlRs.getString(8);
                        String SST = mysqlRs.getString(9);
                        String SZIP = mysqlRs.getString(10);
                        String SCNTRY = mysqlRs.getString(11);
                        String BBUS = mysqlRs.getString(12);
                        String BFIRST = mysqlRs.getString(13);
                        String BLAST = mysqlRs.getString(14);
                        String BADD1 = mysqlRs.getString(15);
                        String BADD2 = mysqlRs.getString(16);
                        String BCITY = mysqlRs.getString(17);
                        String BST = mysqlRs.getString(18);
                        String BZIP = mysqlRs.getString(19);
                        String BCNTRY = mysqlRs.getString(20);
                        String EMAIL = mysqlRs.getString(21);
                        String DPHONE = mysqlRs.getString(22);
                        String EPHONE = mysqlRs.getString(23);
                        String SHPMTH = mysqlRs.getString(24);
                        String SHPDES = mysqlRs.getString(25);
                        String SCOST = mysqlRs.getString(26);
                        Double WTOTAL = mysqlRs.getDouble(27);
                        String RCVON = mysqlRs.getString(28);
                        String PRODUCTS = mysqlRs.getString(29);
                        Double SUBTOTAL = mysqlRs.getDouble(30);



                        double otherWeights = 2.4;  // other weights
                        String mySort = "1";
                        if (SHPMTH.equals("FS")){
                            mySort = "1";
                        }

                        if (SHPMTH.contains("F1")){
                            mySort = "2";
                        }

                        if (SHPMTH.contains("F2")){
                            mySort = "3";
                        }

                        // USPS1 is up to 1 lb.
                        if (SHPMTH.equals("USPS1")){
                            otherWeights = 1.4;
                            mySort = "4";
                        }
                        log.info("otherWeights: " + otherWeights);



                        // ------- log this record ------------------------------------
                        ResultSetMetaData rsMetaData = mysqlRs.getMetaData();
                        int numberOfColumns = rsMetaData.getColumnCount();
                        //32
                        String recordString = "";
                        for (int i = 1; i < numberOfColumns + 1; i++) {
                            if (rsMetaData.getColumnTypeName(i).equals("VARCHAR") || rsMetaData.getColumnTypeName(i).equals("CHAR")){
                                recordString += rsMetaData.getColumnName(i) + "=" + mysqlRs.getString(i) + "|";
                            }
                            else if (rsMetaData.getColumnTypeName(i).equals("DECIMAL")){
                                recordString += rsMetaData.getColumnName(i) + "=" + mysqlRs.getDouble(i) + "|";
                            }


                            else{
                                log.info("Unknown column type: " + rsMetaData.getColumnTypeName(i));
                            }
                        }
                        log.info("Record read: " + recordString);
                        // ==================================================================

                        

                        String DPHONEout = "";
                        for (int i = 0;i < DPHONE.length();i++){
                            String thisChar = DPHONE.substring(i,i+1);
                            if (thisChar.equals(" ")){
                                // do nothing
                            }
                            else{
                                DPHONEout += thisChar;
                            }
                        }
                        
                        

                        String PHONE = DPHONEout.substring(0,13);
                        String PEXT = "";
                        try{
                            PEXT = DPHONEout.substring(17);
                        }catch(Exception e){
                            log.error("Error getting phone extension");
                        }
                            

                        String as400RCVON = RCVON.substring(0,8);
                        String as400RCVTM = RCVON.substring(8,14);
                        
                        String STATUS = "A";
                        //if (WTOTAL > 500){
                        //    STATUS = "L";
                        //}


                        
                        double totalOunces = 0;
                        double totalPounds = 0;

                        
                        
                        
                        if (customerNumber.equals("8376")){
                            totalOunces = insertTrandStuffOldStyle(customerNumber,CONF, PRODUCTS);
                        } else {
                            totalOunces = insertTrandStuff(customerNumber,CONF, PRODUCTS);
                        }
                        
                        
                        
                        
                        
                        
                        // at this point, I should have the total ounces.
                        ////////////// we still should add the ounces to the tranh.
                        


                        totalOunces += otherWeights;
                        totalPounds = 0;
                        if (totalOunces > 8){
                            totalPounds = totalOunces / 16;
                            if (totalPounds <= 1){totalPounds = 1;}
                            if (totalPounds > 1 && totalPounds <=2){totalPounds = 2;}
                            if (totalPounds > 2 && totalPounds <=3){totalPounds = 3;}
                            if (totalPounds > 3 && totalPounds <=4){totalPounds = 4;}
                            if (totalPounds > 4 && totalPounds <=5){totalPounds = 5;}
                            totalOunces = 0;
                        }
                        

                        // override all previous weights for this special case.
                        // the otherweights and books Judy gave me did not add up.
                       if (customerNumber.equals("8736")){
                            if (SHPMTH.equals("USPS1")){
                                if (lastProdQty == 1){
                                    totalOunces = 1.9;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 2){
                                    totalOunces = 2.9;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 3){
                                    totalOunces = 3.6;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 4){
                                    totalOunces = 4.3;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 5){
                                    totalOunces = 5;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 6){
                                    totalOunces = 5.8;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 7){
                                    totalOunces = 6.6;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 8){
                                    totalOunces = 7.2;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 9){
                                    totalOunces = 7.9;
                                    totalPounds = 0;
                                }
                                if (lastProdQty == 10){
                                    totalOunces = 8.7;
                                    totalPounds = 0;
                                }
                            }
                       }


                        log.info("totalOunces: " + totalOunces);
                        log.info("totalPounds: " + totalPounds);


                        // insert into BATCHF/TRANH.
                        try{
                            Class.forName(as400Driver).newInstance();
                            as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                            //20
                            String as400SQL = "insert into TRANH " +
                                    "(CACCT,EVENT,CONF,SBUS,SFIRST,SLAST,SADD1,SADD2,SCITY,SST,SZIP,SCNTRY,BBUS," +
                                    "BFIRST,EMAIL,PHONE,PEXT,SHPMTH," +
                                    "SHPDES,SCOST, WTOTAL,RCVON,RCVTM,STATUS,BULK,OUNCES,LBS,SORT) " +
                                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                            PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                            as400Statement.setString(1, customerNumber);
                            as400Statement.setString(2, eventCode);
                            as400Statement.setString(3, CONF);
                            as400Statement.setString(4, SBUS);
                            as400Statement.setString(5, SFIRST);
                            as400Statement.setString(6, SLAST);
                            as400Statement.setString(7, SADD1);
                            as400Statement.setString(8, SADD2);
                            as400Statement.setString(9, SCITY);
                            as400Statement.setString(10, SST);
                            as400Statement.setString(11, SZIP);
                            as400Statement.setString(12, SCNTRY);
                            as400Statement.setString(13, BBUS);
                            as400Statement.setString(14, BFIRST);
                            as400Statement.setString(15, EMAIL);
                            as400Statement.setString(16, PHONE);
                            as400Statement.setString(17, PEXT);
                            as400Statement.setString(18, SHPMTH);
                            as400Statement.setString(19, SHPDES);
                            as400Statement.setString(20, SCOST);
                            as400Statement.setDouble(21, WTOTAL);
                            as400Statement.setString(22, as400RCVON);
                            as400Statement.setString(23, as400RCVTM);
                            as400Statement.setString(24, STATUS);
                            as400Statement.setString(25, "N");
                            as400Statement.setDouble(26, totalOunces);
                            as400Statement.setDouble(27, totalPounds);
                            as400Statement.setString(28, mySort);
                            

                            as400Statement.execute();
                            as400Statement.close();
                            log.info("CONF: " + CONF + " added to TRANH");

                        } catch(Exception e){
                            e.printStackTrace();
                            log.error("ERROR WHILE INSERTING INTO AS400 TRANH: " + e);
                            throw(e);
                        }







                        // update TRANSFERRED DATE
                        try{
                            Date rightNow = new Date();
                            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
                            StringBuilder rightNowStringBuilder = new StringBuilder( sdf.format( dateThreeHoursAgo ) );
                            String rightNowString = rightNowStringBuilder.toString();

                            PreparedStatement stmt2 = mysqlConn.prepareStatement("update TRANSACTIONS set TRANSFERRED_DT = ? WHERE CACCT = ? AND event_code = ? AND CONF = ?");
                            stmt2.setString(1, rightNowString);
                            stmt2.setString(2, customerNumber);
                            stmt2.setString(3, eventCode);
                            stmt2.setString(4, CONF);
                            stmt2.executeUpdate();
                            stmt2.close();
                            log.info("TRANSFERRED_DT updated to " + rightNowString);
                        } catch (Exception e){
                            e.printStackTrace();
                            log.error("ERROR WHILE UPDATING TRANSACTIONS: " + e);
                            throw(e);
                        }

                       
                }
                statement.close();
                mysqlConn.close();
            } catch (Exception e){
                e.printStackTrace();
                log.error("ERROR WHILE SELECTING FROM TRANSACTIONS: " + e);
                throw(e);
            }




    }

    
    
    
    
    public double insertTrandStuff(String customerNumber, String CONF, String PRODUCTS) throws Throwable {
        
                        
                double totalOunces = 0;

                
                
                
                
                // build list of products.
                ArrayList<Product> products = new ArrayList<Product>();
                
                String[] productLines = PRODUCTS.split("\\n");
                /*for (int i = 0; i < productLines.length; i++){
                    String[] productKeysAndValues = productLines[i].split("\\&");
                    Product oneProduct = new Product();
                    for (int ii = 0; ii < productKeysAndValues.length; ii++){
                        String[] productWords = productKeysAndValues[ii].split("\\=");
                        oneProduct.setAcct(customerNumber);
                        
                        if (productWords[0].equals("code")){
                            oneProduct.setCode(productWords[1]);
                        }
                        else if (productWords[0].equals("description")){
                            oneProduct.setDescription(productWords[1]);
                        }
                        else if (productWords[0].equals("price")){
                            oneProduct.setPrice(Double.parseDouble(productWords[1]));
                        }
                        else if (productWords[0].equals("qty")){
                            oneProduct.setQty(Integer.parseInt(productWords[1]));
                        }
                        else if (productWords[0].equals("total")){
                            oneProduct.setTotal(Double.parseDouble(productWords[1]));
                        }
                        
                    }
                    products.add(oneProduct);
                }*/
                
                for (int i = 0; i < productLines.length; i++){
                    String[] productKeysAndValues = productLines[i].split("\\&");
                    Product oneProduct = new Product();
                    for (int ii = 0; ii < productKeysAndValues.length; ii++){
                        String[] productWords = productKeysAndValues[ii].split("\\=");
                        oneProduct.setAcct(customerNumber);
                        
                        
                        // currently, we're just using this for CWS. Currently, there is only 1 product.
                        if (productWords[0].equals("prodCode01")){
                            oneProduct.setCode(productWords[1]);
                        }
                        else if (productWords[0].equals("prodName01")){
                            oneProduct.setDescription(productWords[1]);
                        }
                        else if (productWords[0].equals("prodPrice01")){
                            oneProduct.setPrice(Double.parseDouble(productWords[1]));
                        }
                        else if (productWords[0].equals("prodQty01")){
                            oneProduct.setQty(Integer.parseInt(productWords[1]));
                        }
                        else if (productWords[0].equals("prodTotal01")){
                            oneProduct.setTotal(Double.parseDouble(productWords[1]));
                        }
                        
                    }
                    products.add(oneProduct);
                }
                
                
                
                for (Product oneProduct : products){
                    totalOunces += .7; // all are fedEx, so it doesn't matter.

                    if (customerNumber.equals("71656") && oneProduct.getCode().equals("CMB1")){
                        
                        
                        
                        // make 3 products from CMB1.
                        try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into TRAND " +
                                        "(CACCT,CONF,PCODE,PDESC,PCOST,PQTY,TCST) " +
                                        " VALUES(?,?,?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setString(1, customerNumber);
                                as400Statement.setString(2, CONF);
                                as400Statement.setString(3, "ADULT2");
                                as400Statement.setString(4, "Adult (Family Fun Pack)");
                                as400Statement.setDouble(5, oneProduct.getPrice());
                                as400Statement.setInt(6, oneProduct.getQty() * 2);
                                as400Statement.setDouble(7, oneProduct.getTotal());
                                as400Statement.execute();
                                
                                as400Statement.setString(1, customerNumber);
                                as400Statement.setString(2, CONF);
                                as400Statement.setString(3, "CHILD2");
                                as400Statement.setString(4, "Child (Family Fun Pack)");
                                as400Statement.setDouble(5,0.00);
                                as400Statement.setInt(6, oneProduct.getQty()* 2);
                                as400Statement.setDouble(7, 0.00);
                                as400Statement.execute();
                                
                                
                                as400Statement.setString(1, customerNumber);
                                as400Statement.setString(2, CONF);
                                as400Statement.setString(3, "RIDE");
                                as400Statement.setString(4, "Wristband (Family Fun Pack)");
                                as400Statement.setDouble(5,0.00);
                                as400Statement.setInt(6, oneProduct.getQty() * 2);
                                as400Statement.setDouble(7, 0.00);
                                as400Statement.execute();
                                
                                
                                as400Statement.close();
                                log.info("Family fun pack, qty: " + oneProduct.getQty() + " (2 adults, 2 child, 1 ride) added to TRAND");


                         } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 TRAND: " + e);
                                throw(e);
                         }
                        
                        
                        
                        
                    } else {

                        try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into TRAND " +
                                        "(CACCT,CONF,PCODE,PDESC,PCOST,PQTY,TCST) " +
                                        " VALUES(?,?,?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setString(1, customerNumber);
                                as400Statement.setString(2, CONF);
                                as400Statement.setString(3, oneProduct.getCode());
                                as400Statement.setString(4, oneProduct.getDescription());
                                as400Statement.setDouble(5, oneProduct.getPrice());
                                as400Statement.setInt(6, oneProduct.getQty());
                                as400Statement.setDouble(7, oneProduct.getTotal());

                                as400Statement.execute();
                                as400Statement.close();
                                log.info("prodCode: " + oneProduct.getCode() + ", prodQty: " + oneProduct.getQty() + " added to TRAND");

                         } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 TRAND: " + e);
                                throw(e);
                         }
                    }
                     
                     
                     
                }
                
                
                
                
                
                
                


                
                
                return totalOunces;
        
        
    }
    
    
    
    
    public double insertTrandStuffOldStyle(String customerNumber, String CONF, String PRODUCTS) throws Throwable {
        
                        
                double totalOunces = 0;

                
                
                
                ArrayList prodNumbers = new ArrayList();
                // insert all products into TRAND.
                // split all by &
                String[] productKeysAndValues = PRODUCTS.split("\\&");
                for (int i = 0; i < productKeysAndValues.length; i++){
                    String[] productWords = productKeysAndValues[i].split("\\=");
                    //log.info("productWords[0]: " + productWords[0]);
                    //lo.info("productWords[1]: " + productWords[1]);
                    if (productWords[0].substring(0,9).equals("prodTotal")){
                        prodNumbers.add(productWords[0].substring(9));
                    }
                }
                // at this point, I have prodNumbers() - list of all prod Numbers;


                
                for (Iterator it = prodNumbers.iterator(); it.hasNext();){
                    String thisNumber = (String)it.next();
                    //log.info("thisNumber: " + thisNumber);
                    String prodName = "";
                    String prodCode = "";
                    int prodQty = 0;
                    double prodPrice = 0;
                    double prodTotal = 0;
                    double prodOunces = 0;

                    for (int i = 0; i < productKeysAndValues.length; i++){
                        String[] productWords = productKeysAndValues[i].split("\\=");
                        if (productWords[0].equals("prodCode" + thisNumber)){
                            prodCode = productWords[1];
                            if (prodCode.equals("GENADM")){
                                prodOunces += .7;
                            }
                        }
                        if (productWords[0].equals("prodName" + thisNumber)){
                            prodName = productWords[1];
                        }
                        if (productWords[0].equals("prodQty" + thisNumber)){
                            prodQty = Integer.parseInt(productWords[1]);
                            lastProdQty = prodQty;
                        }
                        if (productWords[0].equals("prodPrice" + thisNumber)){
                            prodPrice = Double.parseDouble(productWords[1]);
                        }
                        if (productWords[0].equals("prodTotal" + thisNumber)){
                            prodTotal = Double.parseDouble(productWords[1]);
                        }



                    }



                        totalOunces += prodOunces * prodQty;
                        log.info("prodOunces: " + prodOunces);
                        log.info("totalOunces (so far): " + totalOunces);



                        try{
                                Class.forName(as400Driver).newInstance();
                                as400Conn = DriverManager.getConnection(as400Url,as400UserName, as400Password);
                                //20
                                String as400SQL = "insert into TRAND " +
                                        "(CACCT,CONF,PCODE,PDESC,PCOST,PQTY,TCST) " +
                                        " VALUES(?,?,?,?,?,?,?)";
                                PreparedStatement as400Statement = as400Conn.prepareStatement(as400SQL);

                                as400Statement.setString(1, customerNumber);
                                as400Statement.setString(2, CONF);
                                as400Statement.setString(3, prodCode);
                                as400Statement.setString(4, prodName);
                                as400Statement.setDouble(5, prodPrice);
                                as400Statement.setInt(6, prodQty);
                                as400Statement.setDouble(7, prodTotal);

                                as400Statement.execute();
                                as400Statement.close();
                                log.info("prodCode: " + prodCode + ", prodQty: " + prodQty + " added to TRAND");

                         } catch(Exception e){
                                e.printStackTrace();
                                log.error("ERROR WHILE INSERTING INTO AS400 TRAND: " + e);
                                throw(e);
                         }

                }
                
                return totalOunces;
        
        
    }
    
    
    
    
    
}
