package com.mathieuancelin.iteratees.test;

import com.mathieuancelin.iteratees.F;
import com.mathieuancelin.iteratees.F.Function;
import com.mathieuancelin.iteratees.F.Promise;
import com.mathieuancelin.iteratees.F.Unit;
import com.mathieuancelin.iteratees.Iteratees;
import static com.mathieuancelin.iteratees.Iteratees.*;
import com.mathieuancelin.iteratees.Iteratees.Enumerator;
import com.mathieuancelin.iteratees.Iteratees.Iteratee;
import com.mathieuancelin.iteratees.test.Streams.Event;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class AStreamsTest {
    
    @Test
    public void testStreams() throws Exception {
        Promise<Unit> p = feed("MANAGER", 0, 1000);
        p.await(10, TimeUnit.SECONDS);
    }
    
    public Promise<Unit> feed(final String role, final int lowerBound, final int higherBound) {

        Iteratees.Enumeratee<Streams.Event, Streams.Event> secure = Iteratees.Enumeratee.map(new F.Function<Streams.Event, Streams.Event>() {
            @Override
            public Streams.Event apply(Streams.Event o) {
                for (Streams.SystemStatus status : F.caseClassOf(Streams.SystemStatus.class, o)) {
                    if (role.equals("MANAGER")) {
                        return status;
                    }
                }
                for (Streams.Operation operation : F.caseClassOf(Streams.Operation.class ,o)) {
                    if (operation.level.equals("public")) {
                         return operation;
                    } else {
                        if (role.equals("MANAGER")) {
                            return operation;
                        }
                    }
                }
                return o;
            }
        });

        Iteratees.Enumeratee<Streams.Event, Streams.Event> inBounds = Iteratees.Enumeratee.map(new F.Function<Streams.Event, Streams.Event>() {
            @Override
            public Streams.Event apply(Streams.Event o) {
                for (Streams.SystemStatus status : F.caseClassOf(Streams.SystemStatus.class, o)) {
                    return status;
                }
                for (Streams.Operation operation : F.caseClassOf(Streams.Operation.class ,o)) {
                    if (operation.amount > lowerBound && operation.amount < higherBound) {
                        return operation;
                    }
                }
                return o;
            }
        });
        
        Iteratees.Enumeratee<Streams.Event, String> asJson = Iteratees.Enumeratee.map(new F.Function<Streams.Event, String>() {
            @Override
            public String apply(Streams.Event o) {
                for (Streams.SystemStatus status : F.caseClassOf(Streams.SystemStatus.class, o)) {
                    return "{\"type\":\"status\", \"message\":\"" + status.message + "\"}";
                }
                for (Streams.Operation operation : F.caseClassOf(Streams.Operation.class ,o)) {
                    return "{\"type\":\"operation\", \"amount\":" + operation.amount + ", \"visibility\":\"" + operation.level + "\"}";
                }
                return "";
            }
        });
        
        Iteratee<Event, Unit> consumer = Iteratee.foreach(new Function<Event, Unit>() {
            public Unit apply(Event t) {
                System.out.println("*** " + t.toString());
                return Unit.unit();
            }
        });
        
        Iteratee<String, Unit> strConsumer = Iteratee.foreach(new Function<String, Unit>() {
            public Unit apply(String t) {
                System.out.println("=== " + t);
                return Unit.unit();
            }
        });

        return Streams.events.through(secure, inBounds).through(asJson).applyOn(strConsumer);
    }
    
}
