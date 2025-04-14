package stree.data;

import stree.data.arbitrary.TRDNodeRAPQ;

import java.io.Serializable;
import java.util.*;

public class TaskCluster implements Serializable {
    public static long windowSize;
    public static int nodeVectorSize;
    int root;
    Map<Long, Integer>[] bucket2TimestampAndCount;
//    List<InputTuple<Integer, Integer, String>> initialEdgeList;
    List<TRDNodeRAPQ<Integer>> endNodeList;
    long maxTimestamp;
//    int vectorCount;
    public TaskCluster(int root, long currentTimestamp, TRDNodeRAPQ<Integer> endNode, Map<Long, Integer>[] bucket2TimestampAndCount) {
        this.root = root;
        this.maxTimestamp = currentTimestamp;
        this.bucket2TimestampAndCount = bucket2TimestampAndCount;
        this.endNodeList = new LinkedList<>();
        this.endNodeList.add(endNode);
        endNode.setAttribute_root(root);
//        this.vectorCount = 1;
    }

    public long calculateOverlapLength(TaskCluster currentCluster){
        long overlapLength = 0;
        for (int index = 0; index < currentCluster.getBucket2TimestampAndCount().length; index++) {
            Map<Long, Integer> timestamp2CountMap = currentCluster.getBucket2TimestampAndCount()[index];
            if (timestamp2CountMap != null) {
                for (Map.Entry<Long, Integer> timestamp2Count : timestamp2CountMap.entrySet()) {
                    if (bucket2TimestampAndCount[index] != null) {
                        if (bucket2TimestampAndCount[index].containsKey(timestamp2Count.getKey())) {
                            overlapLength += (long) bucket2TimestampAndCount[index].get(timestamp2Count.getKey()) * timestamp2Count.getValue();
                        }
                    }
                }
            }
        }
        return overlapLength;
    }

    public void addNewInitialTask(TaskCluster newCluster, long currentTimestamp) {
        Map<Long, Integer>[] newTaskBucket2TimestampAndCount = newCluster.getBucket2TimestampAndCount();
        for (int index = 0; index < newTaskBucket2TimestampAndCount.length; index++) {
            Map<Long, Integer> newTimestampAndCount = newTaskBucket2TimestampAndCount[index];
            if (newTimestampAndCount != null) {
                for (Map.Entry<Long, Integer> timestamp2Count : newTimestampAndCount.entrySet()) {
                    if (this.bucket2TimestampAndCount[index] == null)
                        this.bucket2TimestampAndCount[index] = new HashMap<>();
                    this.bucket2TimestampAndCount[index].putIfAbsent(timestamp2Count.getKey(), 0);
                    // 更新group
                    this.bucket2TimestampAndCount[index].computeIfPresent(timestamp2Count.getKey(), (k, v) -> v + timestamp2Count.getValue());
                }
            }
        }

        // Maintain initialEdgeList
        this.endNodeList.addAll(newCluster.getEndNodeList());
//        vectorCount++;

        // Update maxTimestamp
        if (currentTimestamp > maxTimestamp)
            maxTimestamp = currentTimestamp;
    }

    public Map<Long, Integer>[] getBucket2TimestampAndCount() {
        return bucket2TimestampAndCount;
    }

//    public List<InputTuple<Integer, Integer, String>> getInitialEdgeList() {
//        return initialEdgeList;
//    }


    public List<TRDNodeRAPQ<Integer>> getEndNodeList() {
        return endNodeList;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public int getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return "TaskCluster{" +
                "root=" + root +
                ", bucket2TimestampAndCount=" + Arrays.toString(bucket2TimestampAndCount) +
                ", endNodeList=" + endNodeList +
                '}';
    }
}
