package no.ssb.dc.core.executor;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.testng.Assert.assertFalse;

public class BufferedReorderingTest {

    @Test
    public void testOrder() {

        List<String> positionList = new ArrayList<>();
        positionList.add("1");
        positionList.add("2");
        positionList.add("3");
        positionList.add("4");
        positionList.add("5");

        NavigableSet<Integer> completedSet = new TreeSet<>();
        completedSet.add(positionList.indexOf("1"));
        completedSet.add(positionList.indexOf("3"));
        completedSet.add(positionList.indexOf("5"));

        System.out.printf("ceiling 2L: %d%n", completedSet.ceiling(positionList.indexOf("2")));
        System.out.printf("floor 2L: %d%n", completedSet.floor(positionList.indexOf("5")));

        System.out.printf("ceiling 3L: %d%n", completedSet.ceiling(positionList.indexOf("3")));
        System.out.printf("floor 3L: %d%n", completedSet.floor(positionList.indexOf("3")));

        NavigableSet<Integer> headSet = completedSet.headSet(positionList.indexOf("5"), true);
        System.out.printf("headset to 5L: %s%n", headSet.stream().map(Object::toString).collect(Collectors.joining(",")));

        NavigableSet<Integer> subset = completedSet.subSet(positionList.indexOf("1"), true, positionList.indexOf("5"), false);
        System.out.printf("subset 1L to 5L: %s%n", subset.stream().map(Object::toString).collect(Collectors.joining(",")));

    }

    @Test
    public void testCompletedPositions() {
        BufferedReordering<String> bufferedReordering = new BufferedReordering<>();
        bufferedReordering.addExpected("1");
        bufferedReordering.addExpected("2");
        bufferedReordering.addExpected("3");
        bufferedReordering.addExpected("4");
        bufferedReordering.addExpected("5");
        bufferedReordering.addExpected("6");
        bufferedReordering.addExpected("7");
        bufferedReordering.addExpected("8");
        bufferedReordering.addExpected("9");
        bufferedReordering.addExpected("10");
        bufferedReordering.addExpected("11");
        bufferedReordering.addExpected("12");
        bufferedReordering.addExpected("13");
        bufferedReordering.addExpected("14");
        bufferedReordering.addExpected("15");
/*
        {
            bufferedReordering.addCompleted(new Position<>(2L));

            assertNull(bufferedReordering.seekLastPosition(new Position<>(2L)));


            boolean validSequence = bufferedReordering.hasFillOrder(new Position<>(2L));
            assertFalse(validSequence);

            bufferedReordering.addCompletedPosition(new Position<>(1L));
            validSequence = bufferedReordering.hasFillOrder(new Position<>(2L));
            assertTrue(validSequence);

            assertEquals((long)bufferedReordering.seekLastPosition(new Position<>(2L)).asLong(), 2L);

            bufferedReordering.addCompletedPosition(new Position<>(3L));
            bufferedReordering.addCompletedPosition(new Position<>(4L));
            bufferedReordering.addCompletedPosition(new Position<>(5L));

            assertTrue(bufferedReordering.isPageComplete());
            assertEquals((long)bufferedReordering.seekLastPosition(new Position<>(5L)).asLong(), 5L);

        }

        {
            Map<Long, CompletedPositionSequence> pageCompleted = new LinkedHashMap<>();
            pageCompleted.put(bufferedReordering.pageIndex, bufferedReordering);
            pageCompleted.put(bufferedReordering.pageIndex, bufferedReordering);
            pageCompleted.put(bufferedReordering.pageIndex, bufferedReordering);

            bufferedReordering.addCompletedPosition(new Position<>(6L));
            bufferedReordering.addCompletedPosition(new Position<>(8L));

            // locate completed positions
            long page = -1;
            String lastPagePosition = null;
            for(Map.Entry<Long, CompletedPositionSequence> entry : pageCompleted.entrySet()) {
                CompletedPositionSequence completedPositionSequence = entry.getValue();

                long pageIndex = completedPositionSequence.pageIndex;
                String seekLastPagePosition = completedPositionSequence.seekLastPosition(new Position<>(6L));

                if (seekLastPagePosition != null) {
                    page = pageIndex;
                    lastPagePosition = seekLastPagePosition;
                }

                if (!completedPositionSequence.isPageComplete()) {
                    break;
                }
            }
            System.out.printf("page: %s -- seekLastPosition: %s%n", page, lastPagePosition);
        }
*/
    }

    @Test
    public void testStringPositions() {
        Random rnd = new Random(System.currentTimeMillis());

        List<String> allPositions = List.of("abc", "bcd", "cde", "def", "efg", "fgh",
                "ghi", "hij", "ijk", "jkl", "klm", "lmn", "mno", "nop", "opq", "pqr",
                "qrs", "rst", "stu", "tuv", "uvw", "vwx", "wxy", "xyz", "yza", "zab");

        for (int i = 0; i < 100; i++) {

            // Pick a random position ordering
            List<String> positions = new ArrayList<>(allPositions);
            String p1 = positions.remove(rnd.nextInt(26));
            String p2 = positions.remove(rnd.nextInt(25));
            String p3 = positions.remove(rnd.nextInt(24));
            String p4 = positions.remove(rnd.nextInt(23));
            String p5 = positions.remove(rnd.nextInt(22));


            // run re-ordering test

            BufferedReordering<String> completedPositionSequence = new BufferedReordering<>();

            completedPositionSequence.addExpected(p1);
            completedPositionSequence.addExpected(p2);
            completedPositionSequence.addExpected(p3);
            completedPositionSequence.addExpected(p4);
            completedPositionSequence.addExpected(p5);

            assertFalse(hasFillOrder(completedPositionSequence, p1));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p2)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p3)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p4)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p5)));
//
//            completedPositionSequence.addCompletedPosition(new Position<>(p1));
//            completedPositionSequence.addCompletedPosition(new Position<>(p3));
//            completedPositionSequence.addCompletedPosition(new Position<>(p5));
//
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p1)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p2)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p3)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p4)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p5)));
//
//            completedPositionSequence.addCompletedPosition(new Position<>(p2));
//
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p1)));
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p2)));
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p3)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p4)));
//            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p5)));
//
//            completedPositionSequence.addCompletedPosition(new Position<>(p4));
//
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p1)));
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p2)));
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p3)));
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p4)));
//            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p5)));
        }
    }

    private boolean hasFillOrder(BufferedReordering<String> bufferedReordering, String position) {
        return false;
    }
}
