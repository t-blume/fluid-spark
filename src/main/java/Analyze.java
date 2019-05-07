import java.io.Serializable;
import java.util.HashMap;

public class Analyze implements Serializable {

    private HashMap<String, Integer> stats = new HashMap<>();

    public void update(String key, Integer value){
        stats.merge(key, value, (O,N) -> O + N);
    }

    public HashMap<String, Integer> getStats() {
        return stats;
    }

    public static void main(String[] args){
        Analyze analyze = new Analyze();
        System.out.println("asd");
    }
}
