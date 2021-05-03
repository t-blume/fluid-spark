package database;

import instumentation.InstrumentationAgent;

import java.io.Serializable;
import java.util.Set;

public class Imprint implements Serializable {

    public int _id;
    public long _timestamp;
    public Set<String> _payload;
    public int _schemaElementID;

    public long mem_size(boolean payload){
        long size =  InstrumentationAgent.getObjectSize(this);
        size += InstrumentationAgent.getObjectSize(_id);
        size += InstrumentationAgent.getObjectSize(_timestamp);
        size += InstrumentationAgent.getObjectSize(_schemaElementID);
        if (payload){
            size += InstrumentationAgent.getObjectSize(_payload);
            for(String pay : _payload)
                size += InstrumentationAgent.getObjectSize(pay);
        }
        return size;
    }


    public Imprint(int _id, long _timestamp, Set<String> _payload, int _schemaElementID) {
        this._id = _id;
        this._timestamp = _timestamp;
        this._payload = _payload;
        this._schemaElementID = _schemaElementID;
    }


    @Override
    public String toString() {
        return "Imprint{" +
                "_id=" + _id +
                ", _timestamp=" + _timestamp +
                ", _payload=" + _payload +
                ", _schemaElementID=" + _schemaElementID +
                '}';
    }
}
