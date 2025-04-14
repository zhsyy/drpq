package runtime;

import input.InputTuple;
import input.SimpleTextStreamWithExplicitDeletions;
import input.TextFileStream;
import stree.data.AbstractTRDNode;
import stree.query.Automata;
import stree.query.ManualQueryAutomata;
import stree.data.arbitrary.TRDRAPQ;
import stree.data.arbitrary.TRDNodeRAPQ;
import stree.engine.RPQEngine;
import stree.engine.WindowedRPQ;
import stree.util.Constants;
import stree.util.Hasher;
import stree.util.Semantics;
import mpjdev.Status;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import mpi.MPI;
import stree.data.MonitoredNode;
import stree.data.TaskCluster;

public class QueryOnLocal {
    private static Logger logger = LoggerFactory.getLogger(QueryOnLocal.class);
    public static void main(String[] args) {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int rankSize = MPI.COMM_WORLD.Size();

        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command cmd argument can NOT be parsed", e);
            return;
        }

        // parse all necessary command cmd options
        String readFilePath = cmd.getOptionValue("rf", "");
        boolean ifWriteTaskList = cmd.hasOption("wf");
        int hop = Integer.parseInt(cmd.getOptionValue("hop", "1"));
        int groupingThreshold = Integer.parseInt(cmd.getOptionValue("thr", "0"));
        int nodeVectorSize = Integer.parseInt(cmd.getOptionValue("nvs", "100"));
        String filePath = cmd.getOptionValue("fp", "./src/main/resources/");
        String fileName = cmd.getOptionValue("f", "sample") + ".txt";
        String queryCase = cmd.getOptionValue("q", "q_0");
        int maxSize = Integer.parseInt(cmd.getOptionValue("ms", "300000"));
        long windowSize = Long.parseLong(cmd.getOptionValue("ws", "7"));
        long slideSize = Long.parseLong(cmd.getOptionValue("ss", "1"));
        int threadCount = Integer.parseInt(cmd.getOptionValue("tc", "10"));
        int partialMatchThreshold = Integer.parseInt(cmd.getOptionValue("pmthr", "1"));
        ManualQueryAutomata<String> query = ManualQueryAutomata.getManualQuery(queryCase);
        TaskCluster.windowSize = windowSize;
        TaskCluster.nodeVectorSize = nodeVectorSize;

        if (rank == 0) {
            // 1. Calculate a cluster center for each timestamp to facilitate modification during sliding window
            // 2. Calculate a cluster center for the entire cluster for comparison
            // 3. I want to know which clusters I have
            // 4. Is the number of clusters infinite?
            // 5. Only clusters with the same root can be put into the same cluster
            long begin = System.currentTimeMillis();
            TextFileStream<Integer, Integer, String> stream = new SimpleTextStreamWithExplicitDeletions();
            stream.open(filePath + fileName, maxSize);

            // Record the number of tuples that have not been processed for each worker, which is used to evaluate the worker's workload
            int[] worker2Workload = new int[rankSize];
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

            // Read the file and generate cachedInputArray and initialTuplesQueue
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
            ExecutorService executor = Executors.newFixedThreadPool(30);
            // Use multithreading to concurrently calculate k hop
            for (InputTuple<Integer, Integer, String> initialTuple : initialTuplesQueue) {
                PacketCluster packetCluster = new PacketCluster(partialMatchThreshold, hop, windowSize, query, cachedInputArray, initialTuple);
                futures.add(executor.submit(packetCluster));
            }
            System.out.println("add finish with time " + (System.currentTimeMillis() - cluster_begin) * 1.0 / 1000 + "s");

            Map<Integer, Set<TaskCluster>> root2clusters = new HashMap<>();
            // Add the obtained k hop to the cluster
            while (!futures.isEmpty()) {
                if (futures.peek().isDone()) {
                    try {
                        Collection<TaskCluster> clusterSet = futures.poll().get();
                        if (hop == -10) {
                            // random
                            allTaskList.addAll(clusterSet);
                        } else {
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
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            initialTuplesQueue.clear();
            executor.shutdown();
            System.out.println("clustering time cost = " + (System.currentTimeMillis() - cluster_begin) * 1.0 / 1000 + "s with size=" + allTaskList.size());

            Random random = new Random();
            Collections.shuffle(allTaskList, random);

            // Send all tuples in advance
            ArrayList<InputTuple<Integer, Integer, String>> allTuple = new ArrayList<>();
            Set<Long> allTimestamp = new HashSet<>();
            for (ArrayList<InputTuple<Integer, Integer, String>> tuples : cachedInputArray.values()) {
                allTuple.addAll(tuples);
                for (InputTuple<Integer, Integer, String> tuple : tuples) {
                    allTimestamp.add(tuple.getTimestamp());
                }
            }
            for (int i = 1; i < rankSize; i++) {
                MPI.COMM_WORLD.Isend(allTuple.toArray(), 0, allTuple.size(), MPI.OBJECT, i, Constants.TUPLES);
                worker2AllTimeOfInitialEdge.putIfAbsent(i, allTimestamp);
            }
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // Hot start
            for (int targetWorker = 1; targetWorker < rankSize; targetWorker++) {
                int sendCount = 0;
                List<AbstractTRDNode> Nodes = new ArrayList<>();
                while (!allTaskList.isEmpty()) {
                    TaskCluster taskCluster = allTaskList.poll();
                    sendCount += taskCluster.getEndNodeList().size();
                    Nodes.addAll(taskCluster.getEndNodeList());
                    if (sendCount > 1000) {
                        break;
                    }
                }
                sendQueryTasks(windowSize, cachedInputArray, worker2AllTimeOfInitialEdge, targetWorker, Nodes, worker2Workload);
            }
            System.out.println("Hot Start Send Tuples = " + Arrays.toString(worker2Workload));

            AtomicBoolean ifCollectFreeWorkers = new AtomicBoolean(true);
            Thread collectFreeWorkers = new Thread(() -> {
                // After the hot start is finished, send a signal to tell the worker to record data and start monitoring unFinishedTaskList
                for (int i = 1; i < rankSize; i++) {
                    MPI.COMM_WORLD.Isend(new int[]{}, 0, 0, MPI.INT, i, Constants.MONITOR_WORKLOAD);
                }
                while (ifCollectFreeWorkers.get()) {
                    Status status = MPI.COMM_WORLD.Iprobe(MPI.ANY_SOURCE, Constants.FREE_WORKER);
                    if (status != null) {
                        int[] taskSize = new int[1];
                        MPI.COMM_WORLD.Recv(taskSize, 0, 1, MPI.INT, status.source, Constants.FREE_WORKER);
                        freeWorker2TaskCount.put(status.source, taskSize[0]);
                    }
                }
            });
            collectFreeWorkers.start();

            int callCount = 0;
            while (true) {
                if (!freeWorker2TaskCount.isEmpty()) {
                    if (!allTaskList.isEmpty()) {
                        int targetWorker = -1;
                        targetWorker = freeWorker2TaskCount.keySet().iterator().next();
                        if (targetWorker != -1) {
                            List<AbstractTRDNode> candidateSendInitialEdges = new LinkedList<>();
                            int count = 0;
                            while (!allTaskList.isEmpty()) {
                                List<TRDNodeRAPQ<Integer>> currentTask = allTaskList.poll().getEndNodeList();
                                candidateSendInitialEdges.addAll(currentTask);
                                count += currentTask.size();
                                if (count > 1000)
                                    break;
                            }
                            random = new Random();
                            Collections.shuffle(candidateSendInitialEdges, random);
                            freeWorker2TaskCount.remove(targetWorker);
                            sendQueryTasks(windowSize, cachedInputArray, worker2AllTimeOfInitialEdge, targetWorker, candidateSendInitialEdges, worker2Workload);
                        }
                    } else {
                        //Send NO_MORE_TASK signal
                        System.out.println("send NO_MORE_TASK signal to workers ");
                        for (int i = 1; i < rankSize; i++) {
                            MPI.COMM_WORLD.Isend(new int[]{}, 0, 0, MPI.INT, i, Constants.NO_MORE_TASK);
                        }
                        break;
                    }
                }
            }
//            writeObjectToFile(tupleList, "./savedTuplesInEachWorker/tupleList.obj");
            System.out.println("Final Send Tuples = " + Arrays.toString(worker2Workload) + " call count = " + callCount);
            ifCollectFreeWorkers.set(false);
            for (int i = 1; i < rankSize; i++) {
                MPI.COMM_WORLD.Isend(new int[]{}, 0, 0, MPI.INT, i, Constants.WORKER_FINISH);
            }

            double totalTime = (System.currentTimeMillis() - begin) * 1.0 /1000;
            logger.info("average speed of sending tuples = " + numberOfTuples / totalTime + "tuples/s, time cost = " + totalTime + "s");
        }
        else {
            long begin = System.currentTimeMillis();
            AtomicLong processBegin = new AtomicLong();
            AtomicInteger unProcessedTupleCount = new AtomicInteger(0);
            RPQEngine<String> rapqEngine = new WindowedRPQ<String, TRDRAPQ<Integer>, TRDNodeRAPQ<Integer>>
                    (query, 10000, windowSize, slideSize, threadCount, Semantics.ARBITRARY, rank, unProcessedTupleCount);

            ConcurrentLinkedQueue<InputTuple<Integer, Integer, String>> receivedTuples = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<AbstractTRDNode> receivedNodes = new ConcurrentLinkedQueue<>();
            Thread receiveMsgTag = new Thread(() -> {
                try {
                    boolean isFinish = false;
                    while (!isFinish) {
                        Status status = MPI.COMM_WORLD.Probe(MPI.ANY_SOURCE, MPI.ANY_TAG);
                        int tag = status.tag;
                        switch (tag) {
                            case Constants.INITIAL_NODE:
//                                int[] size = new int[1];
                                AbstractTRDNode[] nodes = new AbstractTRDNode[status.count];
                                MPI.COMM_WORLD.Recv(nodes, 0, status.count, MPI.OBJECT, 0, Constants.INITIAL_NODE);
                                unProcessedTupleCount.addAndGet(status.count);
                                WindowedRPQ.receiveNewTask = true;
                                Collections.addAll(receivedNodes, nodes);
//                                System.out.println("worker " + rank + " receive " + size[0] + " new initial edge and set receiveNewTask to true");
                                break;
                            case Constants.TUPLES:
                                InputTuple<Integer, Integer, String>[] tuples = new InputTuple[status.count];
                                MPI.COMM_WORLD.Recv(tuples, 0, status.count, MPI.OBJECT, 0, Constants.TUPLES);
                                Collections.addAll(receivedTuples, tuples);
                                break;
                            case Constants.MONITOR_WORKLOAD:
                                processBegin.set(System.currentTimeMillis());
                                MPI.COMM_WORLD.Irecv(new int[]{}, 0, status.count, MPI.INT, 0, Constants.MONITOR_WORKLOAD);
                                ((WindowedRPQ) rapqEngine).beginMonitorTaskList();
//                            MPI.COMM_WORLD.Gather(new int[]{rapqEngine.getUnFinishedTaskListSize()}, 0, 1, MPI.INT, new int[rankSize], 0, 1, MPI.INT, 0);
                                break;
                            case Constants.WORKER_FINISH:
                                MPI.COMM_WORLD.Irecv(new int[]{}, 0, status.count, MPI.INT, 0, Constants.WORKER_FINISH);
//                                System.out.println(rank + " - receive FINISH tag");
                                isFinish = true;
                                break;
                            case Constants.NO_MORE_TASK:
//                                System.out.println("worker " + rank + " receive NO_MORE_TASK tag");
                                MPI.COMM_WORLD.Irecv(new int[]{}, 0, status.count, MPI.INT, 0, Constants.NO_MORE_TASK);
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    System.out.println("worker " + rank + ": " + e);
                    e.printStackTrace();
                }
            });
            receiveMsgTag.start();

            int numberOfReceivedTuples = 0;
            while (receiveMsgTag.isAlive()) {
                int count = 0;
                while (!receivedNodes.isEmpty()) {
                    AbstractTRDNode node = receivedNodes.poll();
                    numberOfReceivedTuples++;
                    rapqEngine.processNode(node);
                    count++;
                }

                while (!receivedTuples.isEmpty()) {
                    InputTuple<Integer, Integer, String> inputTuple = receivedTuples.poll();
                    rapqEngine.processEdge(inputTuple);
                }
                unProcessedTupleCount.addAndGet(-count);
            }
            rapqEngine.shutDown();
            double processTime = (System.currentTimeMillis() - processBegin.get()) * 1.0 /1000;
            logger.info("worker " + rank + ": average speed of processing tuples = " + numberOfReceivedTuples / processTime + "tuples/s, process time cost = " + processTime + "s." );
//            System.out.println(rapqEngine.getResults().toString());
            System.out.println(rapqEngine.getResults().getResultSize());
        }
            MPI.Finalize();
            System.out.println("rank " + rank +" end!");
            System.exit(0);
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

        worker2Workload[targetWorker] += (nodeList.size() + candidateSendTuples.size());
        MPI.COMM_WORLD.Isend(nodeList.toArray(), 0, nodeList.size(), MPI.OBJECT, targetWorker, Constants.INITIAL_NODE);
        MPI.COMM_WORLD.Isend(candidateSendTuples.toArray(), 0, candidateSendTuples.size(), MPI.OBJECT, targetWorker, Constants.TUPLES);
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
        options.addOption("pmthr", "partial-match-length-threshold", true, "partial match length threshold");
        return options;
    }

    static class PacketCluster implements Callable<Collection<TaskCluster>> {
        int hop;
        int partialMatchThreshold;
        long windowSize;
        Automata<String> automata;
        HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray;
        InputTuple<Integer, Integer, String> initialEdge;

        public PacketCluster(int partialMatchThreshold, int hop, long windowSize, Automata<String> automata, HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray, InputTuple<Integer, Integer, String> initialEdge) {
            this.partialMatchThreshold = partialMatchThreshold;
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
                    generatedTaskCluster.add(calculateCluster(initialEdge.getSource(), initialEdge.getTimestamp(), endNode, allNode.values(), hop));
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
                            MonitoredNode newNode = new MonitoredNode(tuple.getTarget(), targetState, parentNode, edgeTime, windowSize);

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

        private TaskCluster calculateCluster(int root, long currentTimestamp, MonitoredNode endNode, Collection<MonitoredNode> nodes, int hop) {
            Map<Long, Integer>[] bucket2TimestampAndCount = new HashMap[TaskCluster.nodeVectorSize];
            LinkedList<Long> lower = new LinkedList<>();
            LinkedList<Long> upper = new LinkedList<>();
            if (hop != -10) {
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
            }
            mergeIntervals(endNode.getMinTimes(), endNode.getMaxTimes(), lower, upper);
//            System.out.println(endNode.getVertex() + " " + endNode.getState() + " " + lower + " " + upper + " | " + Arrays.toString(bucket2TimestampAndCount));
            return new TaskCluster(root, currentTimestamp, new TRDNodeRAPQ<>(endNode.getVertex(), endNode.getState(), lower, upper), bucket2TimestampAndCount);
        }
    }

    public static void mergeIntervals(List<Long> a, List<Long> b, List<Long> new_a, List<Long> new_b) {
        // Check input validity
        if (a == null || b == null || a.isEmpty() || b.isEmpty() || a.size() != b.size()) {
            return;
        }

        // Use iterators to iterate over a and b
        ListIterator<Long> itA = a.listIterator();
        ListIterator<Long> itB = b.listIterator();

        // Take the first interval as the current merge interval
        long currentStart = itA.next();
        long currentEnd = itB.next();

        // Traverse the remaining interval
        while (itA.hasNext() && itB.hasNext()) {
            long nextStart = itA.next();
            long nextEnd = itB.next();

            // If the starting point of the next interval is within the current interval (or adjacent to the current interval), the two intervals are merged.
            if (nextStart <= currentEnd) {
                currentEnd = Math.max(currentEnd, nextEnd);
            } else {
                // If there is no overlap, add the current merge interval to the result list
                new_a.add(currentStart);
                new_b.add(currentEnd);
                // Update the current interval to the next interval
                currentStart = nextStart;
                currentEnd = nextEnd;
            }
        }
        // Add the last interval to the result list
        new_a.add(currentStart);
        new_b.add(currentEnd);
    }

    public static void writeObjectToFile(Object obj, String filePath) {
        try (FileOutputStream fileOut = new FileOutputStream(filePath);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(obj);
            System.out.println("Object has been serialized to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object readObjectFromFile(String filePath) {
        try (FileInputStream fileIn = new FileInputStream(filePath);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
