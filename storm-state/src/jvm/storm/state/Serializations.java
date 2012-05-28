package storm.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import java.util.ArrayList;
import java.util.List;


public class Serializations {
    public List<Registration> _registrations = new ArrayList<Registration>();
    
    public Serializations add(Class c) {
        _registrations.add(new Registration(c, null));
        return this;
    }

    public Serializations add(Class c, Serializer ser) {
        _registrations.add(new Registration(c, ser));
        return this;
    }
    
    public Serializations clone() {
        Serializations ret = new Serializations();
        ret._registrations = new ArrayList<Registration>(_registrations);
        return ret;
    }

    public void apply(Kryo k) {
        for(Registration r: _registrations) {
            if(r.serializer==null) {
                k.register(r.klass);
            } else {
                k.register(r.klass, r.serializer);
            }
        }
    }
    
    private static class Registration {
        Class klass;
        Serializer serializer;
        
        public Registration(Class klass, Serializer ser) {
            this.klass = klass;
            this.serializer = ser;
        }
    }
}
