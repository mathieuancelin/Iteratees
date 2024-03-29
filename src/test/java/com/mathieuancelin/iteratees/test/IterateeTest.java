/*
 *  Copyright 2011-2012 Mathieu ANCELIN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package com.mathieuancelin.iteratees.test;


import akka.actor.ActorRef;
import com.mathieuancelin.iteratees.F;
import com.mathieuancelin.iteratees.F.Action;
import com.mathieuancelin.iteratees.F.Function;
import com.mathieuancelin.iteratees.F.Option;
import com.mathieuancelin.iteratees.F.Promise;
import com.mathieuancelin.iteratees.F.Unit;
import com.mathieuancelin.iteratees.Iteratees.CharacterEnumerator;
import com.mathieuancelin.iteratees.Iteratees.Cont;
import com.mathieuancelin.iteratees.Iteratees.EOF;
import com.mathieuancelin.iteratees.Iteratees.Elem;
import com.mathieuancelin.iteratees.Iteratees.Enumeratee;
import com.mathieuancelin.iteratees.Iteratees.Enumerator;
import com.mathieuancelin.iteratees.Iteratees.HubEnumerator;
import com.mathieuancelin.iteratees.Iteratees.Iteratee;
import com.mathieuancelin.iteratees.Iteratees.PushEnumerator;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.Assert;
import org.junit.Test;

public class IterateeTest {
    
    @Test
    public void testIterateeOnList() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Promise<String> promise = Enumerator.of("Mathieu", "Kevin", "Jeremy")
                    .applyOn(new ListIteratee());
        promise.onRedeem(new Action<Promise<String>>() {
            @Override
            public void apply(Promise<String> t) {
                try {
                    System.out.println(t.get());
                    Assert.assertEquals(t.get(), "MathieuKevinJeremy");
                    latch.countDown();
                } catch (Exception ex) {}
            }
        });  
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testIterateeOnListThroughEnumeratee() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Promise<String> promise = Enumerator.of("Mathieu", "Kevin", "Jeremy")
            .through(Enumeratee.map(new Function<String, String>() {
                @Override
                public String apply(String t) {
                    return t.toUpperCase();
                }
            })).applyOn(new ListIteratee());
        promise.onRedeem(new Action<Promise<String>>() {
            @Override
            public void apply(Promise<String> t) {
                try {
                    System.out.println(t.get());
                    Assert.assertEquals(t.get(), "MATHIEUKEVINJEREMY");
                    latch.countDown();
                } catch (Exception ex) {}
            }
        });  
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testIterateeOnListThroughEnumeratees() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Promise<String> promise = Enumerator.of("Mathieu", "Kevin", "Jeremy")
            .through(Enumeratee.map(new Function<String, String>() {
                @Override
                public String apply(String t) {
                    return t.toUpperCase();
                }
            }), Enumeratee.map(new Function<String, String>() {
                @Override
                public String apply(String t) {
                    return t + " ===>> ";
                }
            })).applyOn(new ListIteratee());
        promise.onRedeem(new Action<Promise<String>>() {
            @Override
            public void apply(Promise<String> t) {
                try {
                    System.out.println(t.get());
                    Assert.assertEquals(t.get(), "MATHIEU ===>> KEVIN ===>> JEREMY ===>> ");
                    latch.countDown();
                } catch (Exception ex) {}
            }
        });  
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testIterateeOnChar() throws Exception {
        final CountDownLatch latch = new CountDownLatch(Character.MAX_VALUE);
        Promise<Unit> promise = new CharacterEnumerator().applyOn(Iteratee.foreach(new Function<Character, Unit>() {
            @Override
            public Unit apply(Character l) {
                latch.countDown();
                return Unit.unit();
            }
        }));
        latch.await();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testFileEnumerator() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        Enumerator<byte[]> fileEnum = Enumerator.fromFile(new File("src/main/java/com/mathieuancelin/iteratees/Iteratees.java"), 1024);
        Promise<Unit> promise = fileEnum.applyOn(Iteratee.foreach(new Function<byte[], Unit>() {
            @Override
            public Unit apply(byte[] t) {
                for (byte b : t) {
                    System.out.print(b);
                }
                count.incrementAndGet();
                return Unit.unit();
            }
        }));
        promise.get();
        Assert.assertTrue(count.get() > 0);
    }
    
    @Test
    public void testFileLineEnumerator() throws Exception {
        Enumerator<String> fileEnum = Enumerator.fromFileLines(new File("src/main/java/com/mathieuancelin/iteratees/Iteratees.java"));
        final AtomicInteger count = new AtomicInteger(0);
        Promise<Unit> promise = fileEnum.applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                count.incrementAndGet();
                return Unit.unit();
            }
        }));
        promise.get();
        Assert.assertTrue(count.get() > 0);
    }
    
    @Test
    public void testPushEnumerator() throws Exception {
        final CountDownLatch latch = new CountDownLatch(5);
        PushEnumerator<String> pushEnum = Enumerator.unicast(String.class);
        pushEnum.applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                System.out.println(t);
                latch.countDown();
                return Unit.unit();
            }
        }));
        pushEnum.push("Hello dude");
        Thread.sleep(1000);
        pushEnum.push("Hello dude");
        Thread.sleep(2000);
        pushEnum.push("Hello dude");
        Thread.sleep(500);
        pushEnum.push("Hello dude");
        Thread.sleep(1000);
        pushEnum.push("Hello dude");
        latch.await();
        pushEnum.stop();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testInterleave() throws Exception {
        final CountDownLatch latch = new CountDownLatch(5);
        PushEnumerator<String> pushEnum1 = Enumerator.unicast(String.class);
        PushEnumerator<String> pushEnum2 = Enumerator.unicast(String.class);
        PushEnumerator<String> pushEnum3 = Enumerator.unicast(String.class);
        Enumerator<String> global = Enumerator.interleave(pushEnum1, pushEnum2, pushEnum3);
        global.applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                System.out.println(t);
                latch.countDown();
                return Unit.unit();
            }
        }));
        Thread.sleep(1000);
        pushEnum1.push("push1");
        Thread.sleep(1000);
        pushEnum2.push("push2");
        Thread.sleep(1000);
        pushEnum3.push("push3");
        Thread.sleep(1000);
        pushEnum2.push("push2");
        Thread.sleep(1000);
        pushEnum1.push("push1");
        latch.await(7, TimeUnit.SECONDS);
        pushEnum1.stop();
        pushEnum2.stop();
        pushEnum3.stop();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testPushEnumeratorSched2() throws Exception {
        final CountDownLatch latch = new CountDownLatch(200);
        PushEnumerator<String> pushEnum = Enumerator.generate(10, TimeUnit.MILLISECONDS, 
                new Function<Unit, Option<String>>() {
            @Override
            public Option<String> apply(Unit t) {
                latch.countDown();
                return Option.some("Hello dude 20ms");
            } 
        });
        pushEnum.applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                return Unit.unit();
            }
        }));
        latch.await(20, TimeUnit.SECONDS);
        pushEnum.stop();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testPushEnumeratorSched() throws Exception {
        final CountDownLatch latch = new CountDownLatch(10);
        PushEnumerator<String> pushEnum = Enumerator.generate(1, TimeUnit.SECONDS, 
                new Function<Unit, Option<String>>() {
            @Override
            public Option<String> apply(Unit t) {
                latch.countDown();
                return Option.some("Hello dude 1s");
            } 
        });
        pushEnum.applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                System.out.println(t);
                latch.countDown();
                return Unit.unit();
            }
        }));
        latch.await(20, TimeUnit.SECONDS);
        pushEnum.stop();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testHubEnumerator() throws Exception {
        final CountDownLatch latch = new CountDownLatch(6);
        Enumerator<String> enumerator = Enumerator.of("Hello", "World");
        Iteratee<String, Unit> it1 = Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                System.out.println("Received from Iteratee 1 : " + t);
                return Unit.unit();
            }
        });
        Iteratee<String, Unit> it2 = Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                System.out.println("Received from Iteratee 2 : " + t);
                return Unit.unit();
            }
        });
        Iteratee<String, Unit> it3 = Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                System.out.println("Received from Iteratee 3 : " + t);
                return Unit.unit();
            }
        });
        HubEnumerator<String> hub = Enumerator.broadcast(enumerator, false).add(it1).add(it2).add(it3);
        hub.broadcast();
        latch.await();
        hub.stop();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testHubEnumerator2() throws Exception {
        final CountDownLatch latch = new CountDownLatch(60);
        Enumerator<String> enumerator = Enumerator.generate(10, TimeUnit.MILLISECONDS, new Function<Unit, Option<String>>() {
            @Override
            public Option<String> apply(Unit t) {
                String date = System.currentTimeMillis() + "";
                return Option.apply(date);
            }
        });
        Iteratee<String, Unit> it1 = Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                System.out.println("Received from Iteratee 1 : " + t);
                return Unit.unit();
            }
        });
        Iteratee<String, Unit> it2 = Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                System.out.println("Received from Iteratee 2 : " + t);
                return Unit.unit();
            }
        });
        Iteratee<String, Unit> it3 = Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                latch.countDown();
                System.out.println("Received from Iteratee 3 : " + t);
                return Unit.unit();
            }
        });
        HubEnumerator<String> hub = Enumerator.broadcast(enumerator, false).add(it1).add(it2).add(it3);
        hub.broadcast();
        latch.await(10, TimeUnit.SECONDS);
        hub.stop();
        Assert.assertEquals(0, latch.getCount());
    }
    
    @Test
    public void testFileRW() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        final StringBuilder builder = new StringBuilder();
        final StringBuilder builder2 = new StringBuilder();
        File f = new File("/tmp/testiteratee");
        if (!f.exists()) {
            f.createNewFile();
        }
        Enumerator.fromFile(new File("pom.xml"), 1024)
                .applyOn(Iteratee.toStream(new FileOutputStream(f))).get();
        Enumerator.fromFileLines(f).applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                count.incrementAndGet();
                builder2.append(t);
                return Unit.unit();
            }
        })).get();
        Enumerator.fromFileLines(new File("pom.xml")).applyOn(Iteratee.foreach(new Function<String, Unit>() {
            @Override
            public Unit apply(String t) {
                builder.append(t);
                return Unit.unit();
            }
        })).get();
        Assert.assertTrue(count.get() > 0);
        Assert.assertEquals("Damn !!!", builder.toString().trim(), builder2.toString().trim());
        f.delete();
    }

    public static class ListIteratee extends Iteratee<String, String> {
        
        private StringBuilder builder = new StringBuilder();

        @Override
        public void onReceive(Object msg, ActorRef sender, ActorRef self) throws Exception {
            for (Elem e : F.caseClassOf(Elem.class, msg)) {
                Elem<String> el = (Elem<String>) e;
                for (String s : el.get()) {
                    builder.append(s);
                }
                sender.tell(Cont.INSTANCE, self);
            }
            for (EOF e : F.caseClassOf(EOF.class, msg)) {
                done(builder.toString(), sender, self);
            }
        }
    }     
}
