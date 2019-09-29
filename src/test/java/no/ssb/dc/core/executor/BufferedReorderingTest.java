package no.ssb.dc.core.executor;

import no.ssb.dc.api.Position;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class BufferedReorderingTest {

    @Test
    public void testOrder() {

        List<Position<?>> positionList = new ArrayList<>();
        positionList.add(new Position<>(1L));
        positionList.add(new Position<>(2L));
        positionList.add(new Position<>(3L));
        positionList.add(new Position<>(4L));
        positionList.add(new Position<>(5L));

        NavigableSet<Integer> completedSet = new TreeSet<>();
        completedSet.add(positionList.indexOf(new Position<>(1L)));
        completedSet.add(positionList.indexOf(new Position<>(3L)));
        completedSet.add(positionList.indexOf(new Position<>(5L)));

        System.out.printf("ceiling 2L: %d%n", completedSet.ceiling(positionList.indexOf(new Position<>(2L))));
        System.out.printf("floor 2L: %d%n", completedSet.floor(positionList.indexOf(new Position<>(5L))));

        System.out.printf("ceiling 3L: %d%n", completedSet.ceiling(positionList.indexOf(new Position<>(3L))));
        System.out.printf("floor 3L: %d%n", completedSet.floor(positionList.indexOf(new Position<>(3L))));

        NavigableSet<Integer> headSet = completedSet.headSet(positionList.indexOf(new Position<>(5L)), true);
        System.out.printf("headset to 5L: %s%n", headSet.stream().map(p -> p.toString()).collect(Collectors.joining(",")));

        NavigableSet<Integer> subset = completedSet.subSet(positionList.indexOf(new Position<>(1L)), true, positionList.indexOf(new Position<>(5L)), false);
        System.out.printf("subset 1L to 5L: %s%n", subset.stream().map(p -> p.toString()).collect(Collectors.joining(",")));

    }

    @Test
    public void testCompletedPositions() {
        BufferedReordering<Position<?>> bufferedReordering = new BufferedReordering<>();
        bufferedReordering.addExpected(new Position<>(1L));
        bufferedReordering.addExpected(new Position<>(2L));
        bufferedReordering.addExpected(new Position<>(3L));
        bufferedReordering.addExpected(new Position<>(4L));
        bufferedReordering.addExpected(new Position<>(5L));
        bufferedReordering.addExpected(new Position<>(6L));
        bufferedReordering.addExpected(new Position<>(7L));
        bufferedReordering.addExpected(new Position<>(8L));
        bufferedReordering.addExpected(new Position<>(9L));
        bufferedReordering.addExpected(new Position<>(10L));
        bufferedReordering.addExpected(new Position<>(11L));
        bufferedReordering.addExpected(new Position<>(12L));
        bufferedReordering.addExpected(new Position<>(13L));
        bufferedReordering.addExpected(new Position<>(14L));
        bufferedReordering.addExpected(new Position<>(15L));
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
            Position<?> lastPagePosition = null;
            for(Map.Entry<Long, CompletedPositionSequence> entry : pageCompleted.entrySet()) {
                CompletedPositionSequence completedPositionSequence = entry.getValue();

                long pageIndex = completedPositionSequence.pageIndex;
                Position<?> seekLastPagePosition = completedPositionSequence.seekLastPosition(new Position<>(6L));

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
            List<String> positions = new ArrayList(allPositions);
            String p1 = positions.remove(rnd.nextInt(26));
            String p2 = positions.remove(rnd.nextInt(25));
            String p3 = positions.remove(rnd.nextInt(24));
            String p4 = positions.remove(rnd.nextInt(23));
            String p5 = positions.remove(rnd.nextInt(22));


            // run re-ordering test

            /*
            CompletedPositionSequence completedPositionSequence = new CompletedPositionSequence(null);

            completedPositionSequence.addExpected(new Position<>(p1));
            completedPositionSequence.addExpected(new Position<>(p2));
            completedPositionSequence.addExpected(new Position<>(p3));
            completedPositionSequence.addExpected(new Position<>(p4));
            completedPositionSequence.addExpected(new Position<>(p5));

            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p1)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p2)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p3)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p4)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p5)));

            completedPositionSequence.addCompletedPosition(new Position<>(p1));
            completedPositionSequence.addCompletedPosition(new Position<>(p3));
            completedPositionSequence.addCompletedPosition(new Position<>(p5));

            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p1)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p2)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p3)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p4)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p5)));

            completedPositionSequence.addCompletedPosition(new Position<>(p2));

            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p1)));
            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p2)));
            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p3)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p4)));
            assertFalse(completedPositionSequence.hasFillOrder(new Position<>(p5)));

            completedPositionSequence.addCompletedPosition(new Position<>(p4));

            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p1)));
            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p2)));
            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p3)));
            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p4)));
            assertTrue(completedPositionSequence.hasFillOrder(new Position<>(p5)));

             */
        }
    }
}
