import org.apache.log4j.*;
import org.slf4j.LoggerFactory;


public class Test {


    public static void main(String[] args){
        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.INFO);
        console.activateOptions();
        //add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(console);

        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile("logs/updates.log");
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setThreshold(Level.DEBUG);
        fa.setAppend(true);
        fa.activateOptions();

        //add appender to any Logger (here is root)
        Logger.getLogger("UPDATES").addAppender(fa);
        //repeat with all other desired appenders



        org.slf4j.Logger fizz = LoggerFactory.getLogger("UPDATES");

        LoggerFactory.getLogger("other").info("should not appear");
        fizz.info("test");

    }
}
