package com.mathieuancelin.iteratees.test;

import static com.mathieuancelin.iteratees.F.*;
import com.mathieuancelin.iteratees.F.Function;
import com.mathieuancelin.iteratees.F.Promise;
import com.mathieuancelin.iteratees.F.Unit;
import static com.mathieuancelin.iteratees.Iteratees.*;
import com.mathieuancelin.iteratees.Iteratees.Iteratee;
import static com.mathieuancelin.iteratees.test.Streams.*;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class AStreamsTest {
    
    @Test
    public void testStreams() throws Exception {
        //Promise<Unit> p = feed("MANAGER", 0, 1000);
        //p.await(10, TimeUnit.SECONDS);
    }
    
    public Promise<Unit> feed(final String role, final int lowerBound, final int higherBound) {

        Enumeratee<Event, Event> secure = Enumeratee.collect(new Function<Event, Option<Event>>() {
            @Override
            public Option<Event> apply(Event o) {
                for (SystemStatus status : caseClassOf(SystemStatus.class, o)) {
                    if (role.equals("MANAGER")) {
                        return Option.<Event>some(status);
                    }
                }
                for (Operation operation : caseClassOf(Operation.class, o)) {
                    if (operation.level.equals("public")) {
                        return Option.<Event>some(operation);
                    } else {
                        if (role.equals("MANAGER")) {
                            return Option.<Event>some(operation);
                        }
                    }
                }
                return Option.none();
            }
        });

        Enumeratee<Event, Event> inBounds = Enumeratee.collect(new Function<Event, Option<Event>>() {
            @Override
            public Option<Event> apply(Event o) {
                for (SystemStatus status : caseClassOf(SystemStatus.class, o)) {
                    return Option.<Event>some(status);
                }
                for (Operation operation : caseClassOf(Operation.class ,o)) {
                    if (operation.amount > lowerBound && operation.amount < higherBound) {
                        return Option.<Event>some(operation);
                    }
                }
                return Option.none();
            }
        });

        Enumeratee<Event, String> asJson = Enumeratee.map(new Function<Event, String>() {
            @Override
            public String apply(Event o) {
                for (SystemStatus status : caseClassOf(SystemStatus.class, o)) {
                    return "{\"type\":\"status\", \"message\":\"" + status.message + "\"}";
                }
                for (Operation operation : caseClassOf(Operation.class ,o)) {
                    return "{\"type\":\"operation\", \"amount\":" + operation.amount + ", \"visibility\":\"" + operation.level + "\"}";
                }
                return "";
            }
        });
        
        Iteratee<String, Unit> strConsumer = Iteratee.foreach(new Function<String, Unit>() {
            public Unit apply(String t) {
                System.out.println(t);
                return Unit.unit();
            }
        });

        return Streams.events.through(secure, inBounds).through(asJson).applyOn(strConsumer);
    }
    
}
