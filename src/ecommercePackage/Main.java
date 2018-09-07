/*
DESCRIPTION: 
    jdbc example migrating data MySQL to AS400 (remote ip address). 
    

RUNNING:
    Needs to be executed on linux machine hosting MySQL as shell script
        ideally from crontab scheduler.

TODO (2018-09-06): 
   * needs to be refactored for example purposes. 
   * needs hamcrest library added to project
   
 */


package ecommercePackage;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import org.apache.log4j.*;
import org.apache.log4j.PropertyConfigurator;




/**
 *
 * @author travis
 */
public class Main {



     /* Constructor */
    public Main() {
        
    } //end constructor



    public static void main(String[] args) throws Throwable  {

        // the routine below only works for cws at this point.
        
        RefreshControlTables ra = new RefreshControlTables();
        ra.refreshAccounts();
        ra.refreshEvents();
        ra.refreshProducts();
        ra.refreshBarcodedSeasonPasses();
        
        
        UploadCustomer uc = new UploadCustomer();
        
        uc.uploadOneCustomer("8736");
        uc.uploadOneCustomer("71656");

    }


    
    
    
    
    
    
    
}
