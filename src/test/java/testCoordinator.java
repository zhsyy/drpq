import input.InputTuple;
import input.SimpleTextStreamWithExplicitDeletions;
import input.TextFileStream;
import stree.data.AbstractTRDNode;
import stree.data.MonitoredNode;
import stree.data.TaskCluster;
import stree.data.arbitrary.TRDNodeRAPQ;
import stree.query.Automata;
import stree.query.ManualQueryAutomata;
import stree.util.Constants;
import stree.util.Hasher;
import mpi.MPI;
import org.apache.commons.cli.Options;

import java.util.*;
import java.util.concurrent.*;

public class testCoordinator {
    public static void main(String[] args) {
        TextFileStream<Integer, Integer, String> stream = new SimpleTextStreamWithExplicitDeletions();
        stream.open("./src/main/resources/test.txt", 300000);
        ManualQueryAutomata<String> query = ManualQueryAutomata.getManualQuery("q_0");
        long windowSize = 4;
        int hop = 1;
        int groupingThreshold = 1;
        TaskCluster.windowSize = windowSize;
        TaskCluster.nodeVectorSize = 5;

        // Record the number of tuples that have not been processed for each worker, which is used to evaluate the worker's workload
        // Record the tuple corresponding to each timestamp to supplement the tuple before sending the initial edge
        HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray = new HashMap<>();
        // Record the number of tuples corresponding to each timestamp
        HashMap<Long, Integer> time2NumberOfTuples = new HashMap<>();
        // worker -> {max time of initial edge} The maximum timestamp of the initial edge on each worker, used to calculate duplicate edges
        HashMap<Integer, Set<Long>> worker2AllTimeOfInitialEdge = new HashMap<>();

        int numberOfTuples = 0;
        InputTuple<Integer, Integer, String> input;
        input = stream.next();
        LinkedList<InputTuple<Integer, Integer, String>> initialTuplesQueue = new LinkedList<>();
        ConcurrentHashMap<Integer, Integer> freeWorker2TaskCount = new ConcurrentHashMap<>();

        LinkedList<TaskCluster> allTaskList = new LinkedList<>();

        // 读取文件，生成cachedInputArray和initialTuplesQueue
        while (input != null) {
            numberOfTuples++;
            long currentTime = input.getTimestamp();
            if (query.isInitialEdge(input.getLabel())) {
                initialTuplesQueue.add(input);
            }
            cachedInputArray.putIfAbsent(currentTime, new ArrayList<>());
            cachedInputArray.get(currentTime).add(input);

            time2NumberOfTuples.putIfAbsent(currentTime, 0);
            time2NumberOfTuples.compute(currentTime, (k, v) -> v + 1);

            input = stream.next();
        }
        // read file close
        stream.close();

        long cluster_begin = System.currentTimeMillis();
        LinkedList<Future<Collection<TaskCluster>>> futures = new LinkedList<>();
        ExecutorService executor = Executors.newFixedThreadPool(25);
        // Use multithreading to concurrently calculate k hop
        for (InputTuple<Integer, Integer, String> initialTuple : initialTuplesQueue) {
            PacketCluster packetCluster = new PacketCluster(hop, windowSize, query, cachedInputArray, initialTuple);
            futures.add(executor.submit(packetCluster));
        }
        System.out.println("add finish with time " + (System.currentTimeMillis() - cluster_begin) * 1.0 / 1000 + "s");

        Map<Integer, Set<TaskCluster>> root2clusters = new HashMap<>();
        // Add the obtained k hop to the cluster
        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                try {
                    Collection<TaskCluster> clusterSet = futures.poll().get();
                    for (TaskCluster currentCluster : clusterSet) {
                        long currentTimestamp = currentCluster.getMaxTimestamp();
                        int root = currentCluster.getRoot();
                        if (root2clusters.containsKey(root)) {
                            TaskCluster targetCluster = null;
                            long targetLength = Long.MIN_VALUE;
                            Set<TaskCluster> clusters = root2clusters.get(root);
                            for (TaskCluster cluster : clusters) {
                                if (cluster.getMaxTimestamp() >= currentTimestamp - windowSize) {
                                    long overlapLength = cluster.calculateOverlapLength(currentCluster);
                                    if (overlapLength > targetLength) {
                                        targetLength = overlapLength;
                                        targetCluster = cluster;
                                    }
                                }
                            }
                            if (targetCluster != null && targetLength >= groupingThreshold
                            ) {
                                targetCluster.addNewInitialTask(currentCluster, currentTimestamp);
                            } else {
                                clusters.add(currentCluster);
                                allTaskList.add(currentCluster);
                            }
                        } else {
                            // Create a new cluster
                            root2clusters.put(root, new HashSet<>());
                            root2clusters.get(root).add(currentCluster);
                            allTaskList.add(currentCluster);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        initialTuplesQueue.clear();
        executor.shutdown();
        System.out.println("clustering time cost = " + (System.currentTimeMillis() - cluster_begin) * 1.0 / 1000 + "s with size=" + allTaskList.size());
        System.out.println("allTaskList : " + allTaskList);
//        System.out.println();

        // Send all tuples in advance
        ArrayList<InputTuple<Integer, Integer, String>> allTuple = new ArrayList<>();
        Set<Long> allTimestamp = new HashSet<>();
        for (ArrayList<InputTuple<Integer, Integer, String>> tuples : cachedInputArray.values()) {
            allTuple.addAll(tuples);
            for (InputTuple<Integer, Integer, String> tuple : tuples) {
                allTimestamp.add(tuple.getTimestamp());
            }
        }
        for (int i = 1; i < 2; i++) {
//            MPI.COMM_WORLD.Isend(allTuple.toArray(), 0, allTuple.size(), MPI.OBJECT, i, Constants.TUPLES);
            worker2AllTimeOfInitialEdge.putIfAbsent(i, new HashSet<>());
        }

//        // Hot start
        for (int targetWorker = 1; targetWorker < 2; targetWorker++) {
            int sendCount = 0;
            List<AbstractTRDNode> Nodes = new ArrayList<>();
            while (!allTaskList.isEmpty()) {
                TaskCluster taskCluster = allTaskList.poll();
                sendCount += taskCluster.getEndNodeList().size();
                Nodes.addAll(taskCluster.getEndNodeList());
                if (sendCount > 1) {
                    break;
                }
            }
            sendQueryTasks(windowSize, cachedInputArray, worker2AllTimeOfInitialEdge, targetWorker, Nodes, null);
        }
//        System.out.println("Hot Start Send Tuples = " + Arrays.toString(worker2Workload));
    }

    private static void sendQueryTask(long windowSize, HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray, HashMap<Integer, Set<Long>> worker2AllTimeOfInitialEdge, int targetWorker, InputTuple<Integer, Integer, String> inputTuple, int[] worker2Workload) {
        ArrayList<InputTuple<Integer, Integer, String>> candidateSendTuples = new ArrayList<>();
        // new一个新的的目的是为了不修改cachedInputArray中的tuple
        inputTuple = new InputTuple<>(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
        inputTuple.setInitialEdge(true);
        candidateSendTuples.add(inputTuple);
        // max(t_1) <= currentTime <= min(t_2)
        long currentTime = inputTuple.getTimestamp();
        // 最大的小于currentTime的ts
        long t_1 = Long.MIN_VALUE;
        // 最小的大于currentTime的ts
        long t_2 = Long.MAX_VALUE;
        Set<Long> tsList = worker2AllTimeOfInitialEdge.get(targetWorker);
        if (tsList != null) {
            for (Long ts : tsList) {
                if (ts <= currentTime) {
                    if (ts > t_1)
                        t_1 = ts;
                }
                if (ts >= currentTime) {
                    if (ts < t_2)
                        t_2 = ts;
                }
            }
        }

        for (long k = Long.max(currentTime - windowSize, t_1 + windowSize + 1); k <= Long.min(currentTime + windowSize, t_2 - windowSize - 1); k++) {
            if (cachedInputArray.containsKey(k)) {
                candidateSendTuples.addAll(cachedInputArray.get(k));
            }
        }
        worker2Workload[targetWorker] += candidateSendTuples.size();
        MPI.COMM_WORLD.Isend(new int[]{1}, 0, 1, MPI.INT, targetWorker, Constants.INITIAL_NODE);
        MPI.COMM_WORLD.Isend(candidateSendTuples.toArray(), 0, candidateSendTuples.size(), MPI.OBJECT, targetWorker, Constants.TUPLES);
        worker2AllTimeOfInitialEdge.putIfAbsent(targetWorker, new HashSet<>());
        worker2AllTimeOfInitialEdge.get(targetWorker).add(currentTime);
        System.out.println("coordinator sends " + 1 + " initial edges with " + candidateSendTuples.size() +  " tuples to worker " + targetWorker);
        if (1 != candidateSendTuples.size())
            System.out.println("not equal!!!");
    }

    private static void sendQueryTasks(long windowSize,
                                       HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray,
                                       HashMap<Integer, Set<Long>> worker2AllTimeOfInitialEdge,
                                       int targetWorker,
                                       List<AbstractTRDNode> nodeList,
                                       int[] worker2Workload) {
        ArrayList<InputTuple<Integer, Integer, String>> candidateSendTuples = new ArrayList<>();
        for (AbstractTRDNode endNode : nodeList) {
            Iterator<Long> lower_bound_timestamp = endNode.lower_bound_timestamp.iterator();
            Iterator<Long> upper_bound_timestamp = endNode.upper_bound_timestamp.iterator();

            Set<Long> validTimestampsForTuple = new HashSet<>();
            while (lower_bound_timestamp.hasNext()) {
                long lower  = lower_bound_timestamp.next();
                long upper  = upper_bound_timestamp.next();
                for (long i = lower; i <= upper; i++) {
                    validTimestampsForTuple.add(i);
                }
            }

            for (Long time : validTimestampsForTuple) {
                for (long k = Long.max(time - windowSize, 0); k <= time; k++) {
                    if (cachedInputArray.containsKey(k) && !worker2AllTimeOfInitialEdge.get(targetWorker).contains(k)) {
                        candidateSendTuples.addAll(cachedInputArray.get(k));
                    }
                    worker2AllTimeOfInitialEdge.putIfAbsent(targetWorker, new HashSet<>());
                    worker2AllTimeOfInitialEdge.get(targetWorker).add(k);
                }
            }
        }

//        worker2Workload[targetWorker] += candidateSendTuples.size();
        System.out.println("nodeList : " + Arrays.toString(nodeList.toArray()));
        System.out.println("candidateSendTuples : "+ Arrays.toString(candidateSendTuples.toArray()));
//        MPI.COMM_WORLD.Isend(nodeList.toArray(), 0, nodeList.size(), MPI.OBJECT, targetWorker, Constants.INITIAL_EDGE);
//        MPI.COMM_WORLD.Isend(candidateSendTuples.toArray(), 0, candidateSendTuples.size(), MPI.OBJECT, targetWorker, Constants.TUPLES);
//        System.out.println("coordinator sends " + inputTuples.size() + " initial edges with " + candidateSendTuples.size() +  " tuples to worker " + targetWorker);

    }

    private static Options getCLIOptions() {
        Options options = new Options();
        options.addOption("alpha", "alpha", true, "alpha");
        options.addOption("hop", "hop", true, "hop");
        options.addOption("thr", "threshold", true, "threshold");
        options.addOption("f", "file", true, "Text file to read");
        options.addOption("fp", "file-path", true, "Directory to store datasets");
        options.addOption("q", "query-case", true, "Query case");
        options.addOption("ms", "max-size", true, "Maximum size to be processed");
        options.addOption("ws", "window-size", true, "Size of the window");
        options.addOption("ss", "slide-size", true, "Slide of the window");
        options.addOption("tc", "threadCount", true, "# of Threads for inter-query parallelism");
        options.addOption("gt", "garbage-collection-threshold", true, "Threshold to execute DGC");
        options.addOption("ft", "fixed-throughput", true, "Fixed fetch rate from dataset");
        options.addOption("tt", "file", false, "Test throughput");
        options.addOption("wf", "file", false, "Write File");
        options.addOption("rf", "file", true, "Read File");
        options.addOption("nvs", "nvs", true, "nvs");
        return options;
    }

    static class PacketCluster implements Callable<Collection<TaskCluster>> {
        int hop;
        int partialMatchThreshold = 0;
        long windowSize;
        Automata<String> automata;
        HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray;
        InputTuple<Integer, Integer, String> initialEdge;

        public PacketCluster(int hop, long windowSize, Automata<String> automata, HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray, InputTuple<Integer, Integer, String> initialEdge) {
            this.hop = hop;
            this.windowSize = windowSize;
            this.automata = automata;
            this.cachedInputArray = cachedInputArray;
            this.initialEdge = initialEdge;
        }

        @Override
        public Collection<TaskCluster> call() {
            List<TaskCluster> generatedTaskCluster = new LinkedList<>();
            // This is the collection of tuples that need to be traversed
            List<InputTuple<Integer, Integer, String>> allCachedTuples = new LinkedList<>();
            for (long i = Long.max(initialEdge.getTimestamp() - windowSize, 0); i <= initialEdge.getTimestamp() + windowSize; i++) {
                if (cachedInputArray.containsKey(i))
                    allCachedTuples.addAll(cachedInputArray.get(i));
            }

            int initialNode = initialEdge.getTarget();

            // 在这里已经使用initialEdge找了一次了
            Map<Integer, Integer> transitions = automata.getTransition(initialEdge.getLabel());
//            System.out.println(transitions.entrySet());
            for (Map.Entry<Integer, Integer> states : transitions.entrySet()) {
                if (states.getKey() != 0)
                    continue;
                MonitoredNode node = new MonitoredNode(initialNode, states.getValue(), initialEdge.getTimestamp()+windowSize, initialEdge.getTimestamp());
                Collection<MonitoredNode> endNodes =  find_K_Length_Partial_Match(allCachedTuples, automata, partialMatchThreshold - 1, node);
//                System.out.println("current node : " + node + "endNodes : " + endNodes);
                for (MonitoredNode endNode : endNodes) {
                    // Traverse hop times
                    Set<Hasher.MapKey<Integer>> cachedNode = new HashSet<>();
                    cachedNode.add(Hasher.createTreeNodePairKey(endNode.getVertex(), endNode.getState()));
                    HashMap<Hasher.MapKey<Integer>, MonitoredNode> allNode = new HashMap<>();
                    allNode.put(Hasher.createTreeNodePairKey(endNode.getVertex(), endNode.getState()), endNode);
                    while (hop > 0){
                        Set<Hasher.MapKey<Integer>> newCachedNode = new HashSet<>();
                        for (InputTuple<Integer, Integer, String> tuple : allCachedTuples) {
                            long edgeTime = tuple.getTimestamp();
                            transitions = automata.getTransition(tuple.getLabel());
                            for (Map.Entry<Integer, Integer> state : transitions.entrySet()) {
                                int sourceState = state.getKey();
                                int targetState = state.getValue();
                                if (cachedNode.contains(Hasher.getThreadLocalTreeNodePairKey(tuple.getSource(), sourceState))){
                                    MonitoredNode parentNode = allNode.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getSource(), sourceState));
                                    if (allNode.containsKey(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState))) {
                                        allNode.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState)).addNewTimes(parentNode, edgeTime, windowSize);
                                        newCachedNode.add(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState));
                                    } else {
                                        // Calculate whether ts intersects
                                        MonitoredNode newNode = new MonitoredNode(tuple.getTarget(), targetState, parentNode, edgeTime, windowSize);
                                        // Only assign a value when it is not empty
                                        if (!newNode.getMaxTimes().isEmpty()) {
                                            if (allNode.containsKey(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState))) {
                                                allNode.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState)).addNewTimes(newNode.getMaxTimes(), newNode.getMinTimes());
                                            } else
                                                allNode.put(Hasher.createTreeNodePairKey(tuple.getTarget(), targetState), newNode);
                                            newCachedNode.add(Hasher.createTreeNodePairKey(tuple.getTarget(), targetState));
                                        }
                                    }
                                }
                            }
                        }
                        cachedNode = newCachedNode;
                        hop--;
                    }
//                    System.out.println("cluster nodes : " + allNode);
                    generatedTaskCluster.add(calculateCluster(initialEdge.getSource(), initialEdge.getTimestamp(), endNode, allNode.values()));
                }
            }
            return generatedTaskCluster;
        }

        private Collection<MonitoredNode> find_K_Length_Partial_Match(List<InputTuple<Integer, Integer, String>> subgraph, Automata<String> automata, int length, MonitoredNode currentNode){
            HashMap<Hasher.MapKey<Integer>, MonitoredNode> returnNodes = new HashMap<>();
            returnNodes.put(Hasher.createTreeNodePairKey(currentNode.getVertex(), currentNode.getState()), currentNode);
            Set<Hasher.MapKey<Integer>> cachedNodes = new HashSet<>();
            cachedNodes.add(Hasher.createTreeNodePairKey(currentNode.getVertex(), currentNode.getState()));
            while (length > 0){
                Set<Hasher.MapKey<Integer>> newCachedNode = new HashSet<>();
                HashMap<Hasher.MapKey<Integer>, MonitoredNode> newReturnNodes = new HashMap<>();
                for (InputTuple<Integer, Integer, String> tuple : subgraph) {
                    long edgeTime = tuple.getTimestamp();
                    Map<Integer, Integer> transitions = automata.getTransition(tuple.getLabel());
                    for (Map.Entry<Integer, Integer> states : transitions.entrySet()) {
                        int sourceState = states.getKey();
                        int targetState = states.getValue();
                        if (cachedNodes.contains(Hasher.getThreadLocalTreeNodePairKey(tuple.getSource(), sourceState))){
                            MonitoredNode parentNode = returnNodes.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getSource(), sourceState));
//                            System.out.println(parentNode);
                            MonitoredNode newNode = new MonitoredNode(tuple.getTarget(), targetState, parentNode, edgeTime, windowSize);
//                            System.out.println( "("+tuple.getTarget() + ", " + targetState + ")" + " | " + parentNode + " | " + edgeTime + " | " + windowSize + " | "+newNode);

                            if (!newNode.getMaxTimes().isEmpty()) {
                                if (newReturnNodes.containsKey(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState))){
                                    newReturnNodes.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState)).addNewTimes(newNode.getMaxTimes(), newNode.getMinTimes());
                                } else {
                                    newReturnNodes.put(Hasher.createTreeNodePairKey(tuple.getTarget(), targetState), newNode);
                                }
                                newCachedNode.add(Hasher.createTreeNodePairKey(tuple.getTarget(), targetState));
                            }
                        }
                    }
                }
                cachedNodes = newCachedNode;
                returnNodes = newReturnNodes;
                length--;
            }
            return returnNodes.values();
        }

        private TaskCluster calculateCluster(int root, long currentTimestamp, MonitoredNode endNode, Collection<MonitoredNode> nodes) {
            Map<Long, Integer>[] bucket2TimestampAndCount = new HashMap[TaskCluster.nodeVectorSize];
            LinkedList<Long> lower = new LinkedList<>();
            LinkedList<Long> upper = new LinkedList<>();
            for (MonitoredNode node : nodes) {
                int index = node.hashCode() % TaskCluster.nodeVectorSize;
                Iterator<Long> iteratorMax = node.getMaxTimes().iterator();
                Iterator<Long> iteratorMin = node.getMinTimes().iterator();
                while (iteratorMax.hasNext()) {
                    long max = iteratorMax.next();
                    long min = iteratorMin.next();
                    for (long i = min; i <= max; i++) {
                        if (bucket2TimestampAndCount[index] == null)
                            bucket2TimestampAndCount[index] = new HashMap<>();
                        bucket2TimestampAndCount[index].putIfAbsent(i, 0);
                        bucket2TimestampAndCount[index].computeIfPresent(i, (k, v) -> v + 1);
                    }
                }
            }
            mergeIntervals(endNode.getMinTimes(), endNode.getMaxTimes(), lower, upper);
            System.out.println(endNode.getVertex() + " " + endNode.getState() + " " + lower + " " + upper + " | " + Arrays.toString(bucket2TimestampAndCount));
            return new TaskCluster(root, currentTimestamp, new TRDNodeRAPQ<>(endNode.getVertex(), endNode.getState(), lower, upper), bucket2TimestampAndCount);
        }
    }

    public static void mergeIntervals(List<Long> a, List<Long> b, List<Long> new_a, List<Long> new_b) {
        // 检查输入有效性
        if (a == null || b == null || a.isEmpty() || b.isEmpty() || a.size() != b.size()) {
            return;
        }

        // 使用迭代器遍历 a 和 b
        ListIterator<Long> itA = a.listIterator();
        ListIterator<Long> itB = b.listIterator();

        // 取第一个区间作为当前合并区间
        long currentStart = itA.next();
        long currentEnd = itB.next();

        // 遍历剩余的区间
        while (itA.hasNext() && itB.hasNext()) {
            long nextStart = itA.next();
            long nextEnd = itB.next();

            // 如果下一区间的起点在当前区间内（或与当前区间相邻），则合并两个区间
            if (nextStart <= currentEnd) {
                currentEnd = Math.max(currentEnd, nextEnd);
            } else {
                // 如果不重叠，将当前合并区间加入结果列表
                new_a.add(currentStart);
                new_b.add(currentEnd);
//                merged.add(new long[]{currentStart, currentEnd});
                // 更新当前区间为下一个区间
                currentStart = nextStart;
                currentEnd = nextEnd;
            }
        }
        // 将最后一个区间加入结果列表
        new_a.add(currentStart);
        new_b.add(currentEnd);
    }
}
