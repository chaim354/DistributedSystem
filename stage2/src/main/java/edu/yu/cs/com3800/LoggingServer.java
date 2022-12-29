package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    default Logger initializeLogging(String fileNamePreface) throws IOException {
        return initializeLogging(fileNamePreface,false);
    }
    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
          //.....
        return createLogger("loggerName",fileNamePreface,disableParentHandlers);
    }

    static Logger createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
        FileHandler fh;  
        Logger logger = Logger.getLogger(loggerName);
    try {  

        // This block configure the logger with handler and formatter  
     // fh = new FileHandler("./src/main/java/edu/yu/cs/com3800/logs/className.log");  
       fh = new FileHandler(fileNamePreface+".log");
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter);  
       // the following statement is used to log any messages  
        //logger.info("My first log");  

    } catch (SecurityException e) {  
        e.printStackTrace();  
    }catch (IOException e) {  
        e.printStackTrace();  
    }  
        if(disableParentHandlers){
            logger.setUseParentHandlers(false);  
        }
        return logger;
    }
}