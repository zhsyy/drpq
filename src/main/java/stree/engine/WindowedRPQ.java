package stree.engine;

import input.InputTuple;
import stree.data.*;
import stree.data.arbitrary.ObjectFactoryArbitrary;
import stree.util.Constants;
import stree.util.RunningSnapShot;
import stree.util.Semantics;
import stree.query.Automata;
import stree.util.Hasher;
import com.codahale.metrics.MetricRegistry;
import mpi.MPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowedRPQ<L, T extends AbstractTRD<Integer, T, N>, N extends AbstractTRDNode<Integer, T, N>> extends RPQEngine<L> {
    private int rank;
    private static int tempCount = 0;
    private static boolean isFinish = false;
    public static boolean receiveNewTask = true;
    private final long windowSize;
    // Default 0
    private final long step;
    private Semantics semantics;
    public Delta<Integer, T, N> delta;
    ObjectFactory<Integer, T, N> objectFactory;
    private final ExecutorService executorService;
    private int numOfThreads;
    private AtomicInteger unProcessedTupleCount;
    public AbstractTRDExpansionJob treeExpansionJob;
    private AtomicInteger unfinishedTaskCount = new AtomicInteger(0);
    private static Logger logger = LoggerFactory.getLogger(WindowedRPQ.class);

    private Thread monitorTaskListCount = new Thread(() -> {
        try {
            while (!isFinish) {
                if (this.unProcessedTupleCount.get() < 100 && this.unfinishedTaskCount.get() <= 50 && receiveNewTask) {
                    MPI.COMM_WORLD.Send(new int[] { this.unfinishedTaskCount.get() }, 0, 1, MPI.INT, 0, Constants.FREE_WORKER);
                    receiveNewTask = false;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error in rank:" + rank);
        }

    });

    private Thread reportTaskListCount = new Thread(() -> {
        try {
            while (!isFinish) {
                System.out.println(rank + " -> " + unfinishedTaskCount.get()
                        + " un_processed_tuples: " + unProcessedTupleCount.get()
                        + " tempCount = "+tempCount);
                TimeUnit.SECONDS.sleep(2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    });

    public void beginMonitorTaskList(){
        monitorTaskListCount.start();
//        reportTaskListCount.start();
    }

    /**
     * Windowed RPQ engine ready to process edges
     * @param query Automata representation of the persistent query
     * @param capacity Initial size for internal data structures. Set to approximate number of edges in a window
     * @param windowSize Size of the sliding window in milliseconds
     * @param step Slide interval in milliseconds
     * @param numOfThreads Total number of executor threads
     * @param semantics Resulting path semantics: @{@link Semantics}
     */
    public WindowedRPQ(Automata<L> query, int capacity, long windowSize, long step, int numOfThreads, Semantics semantics, int rank, AtomicInteger unProcessedTupleCount) {
        super(query, capacity);
        if (semantics.equals(Semantics.ARBITRARY)) {
            this.objectFactory = new ObjectFactoryArbitrary();
        } else {
//            this.objectFactory = new ObjectFactorySimple();
        }
        this.unProcessedTupleCount = unProcessedTupleCount;
        this.rank = rank;
        this.delta = new Delta<>(capacity, objectFactory);
        this.windowSize = windowSize;
        this.step = step;
        this.executorService = Executors.newFixedThreadPool(numOfThreads);
        this.numOfThreads = numOfThreads;
        this.semantics = semantics;
        treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, windowSize, step, false);
        treeExpansionJob.setLatch(unfinishedTaskCount);
    }


    @Override
    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.delta.addMetricRegistry(metricRegistry);
        // call super function to include all other histograms
        super.addMetricRegistry(metricRegistry);
    }


    @Override
    public void processEdge(InputTuple<Integer, Integer, L> inputTuple) {
        long currentTimestamp = inputTuple.getTimestamp();
        Map<Integer, Integer> transitions = automata.getTransition(inputTuple.getLabel());
        if(transitions.isEmpty()) {
            // there is no transition with given label, simply return
            return;
        } else {
            // add edge to the snapshot productGraph
            if (inputTuple.isDeletion())
                productGraph.removeEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
            else
                productGraph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
        }

        List<Map.Entry<Integer, Integer>> transitionList = new ArrayList<>(transitions.entrySet());

        // for each transition that given label satisfy
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            if (!delta.nodeToTRDIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(inputTuple.getSource(), sourceState)))
                continue;

            Collection<T> containingTrees = delta.getTrees(inputTuple.getSource(), sourceState);

            for (T spanningTree : containingTrees) {
                // we do not check target here as even if it exists, we might update its timestamp
                N parentNode = spanningTree.getNodes(inputTuple.getSource(), sourceState);

                if (parentNode != null && parentNode.checkIfMerge(currentTimestamp, windowSize))
                    continue;

                treeExpansionJob.addJob(spanningTree, parentNode, inputTuple.getTarget(), targetState,
                        inputTuple.getTimestamp());
                // check whether the job is full and ready to submit
                if (treeExpansionJob.isFull()) {
                    unfinishedTaskCount.incrementAndGet();
                    executorService.submit(treeExpansionJob);
                    treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata,
                            results, windowSize, step, inputTuple.isDeletion());
                    treeExpansionJob.setLatch(unfinishedTaskCount);
                }
            }
        }
        tempCount++;
    }

    @Override
    public void processNode(AbstractTRDNode node){
        boolean ifProcess = false;
        int root = (int) node.getAttribute_root();

        if (!delta.trdIndex.containsKey(root))
            delta.addLocalTree(root);
        T trd = delta.trdIndex.get(root);

        if (trd.exists((Integer) node.getVertex(), node.getState())) {
            if (trd.getNodes((Integer) node.getVertex(), node.getState()).updateTimestamps(node.lower_bound_timestamp, node.upper_bound_timestamp)) {
                ifProcess = true;
            }
        } else {
            if (trd.nodeIndex.putIfAbsent(Hasher.createTreeNodePairKey((int)node.getVertex(), node.getState()), (N) node) != null) {
                if (trd.getNodes((Integer) node.getVertex(), node.getState()).updateTimestamps(node.lower_bound_timestamp, node.upper_bound_timestamp)) {
                    ifProcess = true;
                }
            } else
                ifProcess = true;
        }

        if (ifProcess) {
            N parentNode = trd.getNodes((Integer) node.getVertex(), node.getState());
            Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(parentNode.getVertex(), parentNode.getState());
            if (forwardEdges != null) {
                // there are forward edges, iterate over them
                for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                    if (parentNode.checkIfMerge(forwardEdge.getTimestamp(), windowSize))
                        continue;
                    // recursive call as the target of the forwardEdge has not been visited in state targetState before

                    treeExpansionJob.addJob(trd, parentNode, forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState(),
                            forwardEdge.getTimestamp());
                    // check whether the job is full and ready to submit
                    if (treeExpansionJob.isFull()) {
                        unfinishedTaskCount.incrementAndGet();
                        executorService.submit(treeExpansionJob);
                        treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata,
                                results, windowSize, step, false);
                        treeExpansionJob.setLatch(unfinishedTaskCount);
                    }
                }
            }
        }
    }

    @Override
    public void shutDown() {
        try {
            unfinishedTaskCount.incrementAndGet();
            executorService.submit(treeExpansionJob);
            while (unfinishedTaskCount.get() != 0) {
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        // shutdown executors
        this.executorService.shutdown();
        this.results.shutDown();
        isFinish = true;
    }
    @Override
    public void record(long ts){
        RunningSnapShot.addRecordTime();
        RunningSnapShot.addTRDSizeSegments(getCountOfSizeOfTRD());
        RunningSnapShot.addUnFinishedTaskSize(getUnFinishedTaskListSize());
//        RunningSnapShot.addRecordTime();
//        delta.record();
//        productGraph.record();
//        RunningSnapShot.addMemUse(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
    }

    /**
     * updates Delta and Spanning Trees and removes any node that is lower than the window endpoint
     * might need to traverse the entire spanning tree to make sure that there does not exists an alternative path
     */
    @Override
    public void GC(long minTimestamp) {
        logger.info("start to GC");
//        while (!futureList.isEmpty()){
//        }

        productGraph.removeOldEdges(minTimestamp - windowSize);
        delta.DGC(minTimestamp, windowSize, this.executorService);
    }

    @Override
    public int getUnFinishedTaskListSize() {
        return unfinishedTaskCount.get();
    }

    public int getNumberOfTRD(){
        return delta.trdIndex.size();
    }

    public int[] getCountOfSizeOfTRD(){
        // 0-10 10-100 100-1000 1000-2000 2000-4000 4000-8000 8000+
        int[] ranges = new int[7];
        for (T trd : delta.trdIndex.values()) {
            int number = trd.getSize();
            if (number >= 8000) {
                ranges[6]++;
            } else if (number >= 4000) {
                ranges[5]++;
            } else if (number >= 2000) {
                ranges[4]++;
            } else if (number >= 1000) {
                ranges[3]++;
            } else if (number >= 100) {
                ranges[2]++;
            } else if (number >= 10) {
                ranges[1]++;
            } else {
                ranges[0]++;
            }
        }
        return ranges;
    }


    @Override
    public int getSizeOfProductGraph(){
        return productGraph.getEdgeCount();
    }
}
