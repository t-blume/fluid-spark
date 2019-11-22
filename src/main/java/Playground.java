import java.io.File;

public class Playground {
    public static void main(String[] args){
        System.out.println("random-test" + " size: " + databaseSize( "random-test"));
        System.out.println("random-test_batch" + " size: " + databaseSize("random-test_batch"));
    }


    public static long databaseSize(String database){
        File dir = new File("orientdb/databases/" + database);
        long size = 0L;
        for (File file : dir.listFiles(F -> F.getName().contains("schema") | F.getName().startsWith("e_") | F.getName().startsWith("v_"))){
           // System.out.println(file.getName());
            size += file.length();
        }
        return size;
    }
}
