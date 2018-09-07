/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package ecommercePackage;


// email as text, html, or attachments, see url below!!!!!!!
// http://www.rgagnon.com/javadetails/java-0321.html
// also see lib folder - need several imports: https://cds.sun.com/is-bin/INTERSHOP.enfinity/WFS/CDS-CDS_Developer-Site/en_US/-/USD/ViewProductDetail-Start?ProductRef=javamail-1.4.2-oth-JPR@CDS-CDS_Developer
// javamail-1.4.2.zip
import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;

/**
 *
 * @author travis
 */
public class TravisMail {


    public String sendHTMLMail(String fromAddress, String replyToAddress, String toAddress, String subject, String body ) throws Exception{
      Properties props = new Properties();
      props.setProperty("mail.transport.protocol", "smtp");
      props.setProperty("mail.host", "mail.wwlnotify.com");
      //props.setProperty("mail.user", "emailuser");
      //props.setProperty("mail.password", "");

      Session mailSession = Session.getDefaultInstance(props, null);
      Transport transport = mailSession.getTransport();

      MimeMessage message = new MimeMessage(mailSession);
      message.setSubject(subject);
      message.setContent
         (body, "text/html; charset=ISO-8859-1");
      message.addRecipient(Message.RecipientType.TO,
         new InternetAddress(toAddress));
      message.setFrom(new InternetAddress(fromAddress));
      
      InternetAddress[] replyTo = new InternetAddress[1];
      replyTo[0] = new InternetAddress(replyToAddress);
      message.setReplyTo(replyTo);
      
    
      transport.connect();
      transport.sendMessage(message,
         message.getRecipients(Message.RecipientType.TO));

      transport.close();
      return "OK";
    }

/*
    public static void sendTextMail(String[] args) throws Exception{
      Properties props = new Properties();
      props.setProperty("mail.transport.protocol", "smtp");
      props.setProperty("mail.host", "mymail.server.org");
      props.setProperty("mail.user", "emailuser");
      props.setProperty("mail.password", "");

      Session mailSession = Session.getDefaultInstance(props, null);
      Transport transport = mailSession.getTransport();

      MimeMessage message = new MimeMessage(mailSession);
      message.setSubject("Testing javamail plain");
      message.setContent("This is a test", "text/plain");
      message.addRecipient(Message.RecipientType.TO,
           new InternetAddress("elvis@presley.org"));

      transport.connect();
      transport.sendMessage(message,
          message.getRecipients(Message.RecipientType.TO));
      transport.close();
    }

  */


   /*  public static void sendAttachmentEmail(String[] args) throws Exception{
      Properties props = new Properties();
      props.setProperty("mail.transport.protocol", "smtp");
      props.setProperty("mail.host", "mymail.server.org");
      props.setProperty("mail.user", "emailuser");
      props.setProperty("mail.password", "");

      Session mailSession = Session.getDefaultInstance(props, null);
      Transport transport = mailSession.getTransport();

      MimeMessage message = new MimeMessage(mailSession);
      message.setSubject("Testing javamail html");
      message.setContent
         ("This is a test <b>HOWTO<b>", "text/html; charset=ISO-8859-1");
      message.addRecipient(Message.RecipientType.TO,
         new InternetAddress("elvis@presley.org"));

      transport.connect();
      transport.sendMessage(message,
         message.getRecipients(Message.RecipientType.TO));
      transport.close();
    }

  */




}
