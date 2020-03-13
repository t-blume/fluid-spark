package database;

public class InvalidComputationException extends Exception {
    InvalidComputationException(){
        super();
    }
    InvalidComputationException(String message){
        super(message);
    }
}
