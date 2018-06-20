package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import com.migu.schedule.server.MiguServer;
import com.migu.schedule.task.MiguTask;

import java.util.*;

import static com.migu.schedule.util.ScheduleUtil.getDValue;

/*
*类名和方法不能修改
 */
public class Schedule {

    private HashMap<Integer,MiguServer> miguServers = new HashMap<>();
    private HashMap<Integer,MiguTask> miguTasks = new HashMap<>();

    public int init() {
        miguServers.clear();
        miguTasks.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        MiguServer server = new MiguServer(nodeId);

        if(nodeId < 0) {
            return ReturnCodeKeys.E004;
        }  else if (miguServers.containsKey(nodeId)) {
            return ReturnCodeKeys.E005;
        } else {
            miguServers.put(nodeId,server);
            return ReturnCodeKeys.E003;
        }
    }

    public int unregisterNode(int nodeId) {
        if(nodeId < 0) {
            return ReturnCodeKeys.E004;
        }  else if (!miguServers.containsKey(nodeId)) {
            return ReturnCodeKeys.E007;
        } else {
            miguServers.remove(nodeId);
            return ReturnCodeKeys.E006;
        }
    }


    public int addTask(int taskId, int consumption) {
        MiguTask task = new MiguTask(taskId,consumption);

        if(taskId <= 0) {
            return ReturnCodeKeys.E009;
        }  else if (miguTasks.containsKey(taskId)) {
            return ReturnCodeKeys.E010;
        } else {
            miguTasks.put(taskId,task);
            return ReturnCodeKeys.E008;
        }
    }


    public int deleteTask(int taskId) {
        if(taskId <= 0) {
            return ReturnCodeKeys.E009;
        }  else if (!miguTasks.containsKey(taskId)) {
            return ReturnCodeKeys.E012;
        } else {
            miguTasks.remove(taskId);
            for(Map.Entry<Integer,MiguServer> entry: miguServers.entrySet()) {
                if(entry.getValue().getTasks().containsKey(taskId)){
                    entry.getValue().removeTask(entry.getValue().getTask(taskId));
                }
            }
            return ReturnCodeKeys.E011;
        }
    }


    public int scheduleTask(int threshold) {

        if(threshold < 0) {
            return ReturnCodeKeys.E002;
        }

        List<MiguServer> optServers = new ArrayList<>();
        List<MiguServer> tmpServers = new ArrayList<>();
        List<MiguTask> currentTasks = new ArrayList<>();
        List<MiguTask> orgTasks = new ArrayList<>();

        for(Map.Entry<Integer,MiguServer> entry: miguServers.entrySet()) {
            MiguServer server = new MiguServer(entry.getValue().getNodeId());
            tmpServers.add(server);
        }

        int totalConsumption = 0;
        int avgConsumption;

        for(Map.Entry<Integer,MiguTask> entry: miguTasks.entrySet()) {
            orgTasks.add(entry.getValue());
            currentTasks.add(entry.getValue());
            totalConsumption += entry.getValue().getConsumption();
        }

        if(totalConsumption % miguServers.size() == 0){
            avgConsumption = totalConsumption / miguServers.size();
        } else {
            avgConsumption = totalConsumption / miguServers.size() + 1;
        }

        Collections.sort(currentTasks, Comparator.comparing(MiguTask::getTaskId));

        int size =  currentTasks.size();

        for(int i =0;i < size; i++) {
            for (int j = 0; j < size; j++) {
                for (int k = 0; k < miguServers.size(); k++) {
                    if (tmpServers.get(k).getTasks().size() == 0) {
                        tmpServers.get(k).setTask(currentTasks.get(j));
                        break;
                    } else {
                        if (tmpServers.get(k).getTotalConsumption() + currentTasks.get(j).getConsumption() <= avgConsumption) {
                            tmpServers.get(k).setTask(currentTasks.get(j));
                            break;
                        } else if (k == miguServers.size() - 1) {//分配到任意节点均超过均值，需要重新分配
                            int minValue = tmpServers.get(0).getTotalConsumption();
                            for (MiguServer server : tmpServers) {
                                minValue = Math.min(minValue, server.getTotalConsumption());
                            }
                            for (MiguServer server : tmpServers) {
                                if (server.getNodeIdByTotalConsumption(minValue) > 0) {
                                    int minCost = minValue;
                                    MiguTask minTask = new MiguTask();
                                    for (Map.Entry<Integer, MiguTask> entry : server.getTasks().entrySet()) {
                                        if (minCost > entry.getValue().getConsumption()) {
                                            minCost = entry.getValue().getConsumption();
                                            minTask = entry.getValue();
                                        }
                                    }
                                    if (minTask.getConsumption() > 0 && server.getTotalConsumption() + currentTasks.get(j).getConsumption() - minCost <= avgConsumption) {
                                        server.setTask(currentTasks.get(j));
                                        server.removeTask(minTask);
                                        int minValue2 = tmpServers.get(0).getTotalConsumption();
                                        for (MiguServer server2 : tmpServers) {
                                            minValue2 = Math.min(minValue2, server2.getTotalConsumption());
                                        }
                                        for (MiguServer server2 : tmpServers) {
                                            if (server2.getNodeIdByTotalConsumption(minValue2) > 0) {
                                                server2.setTask(minTask);
                                                break;
                                            }
                                        }
                                    } else {
                                        server.setTask(currentTasks.get(j));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (optServers.size() == 0) {
                optServers = (List<MiguServer>) ((ArrayList<MiguServer>) tmpServers).clone();
            } else {
                int diffValue1 = getDValue(tmpServers);
                if(diffValue1 <= threshold) {
                    int diffValue2 = getDValue(optServers);
                    if (diffValue2 > diffValue1) {
                        optServers = (List<MiguServer>) ((ArrayList<MiguServer>) tmpServers).clone();
                    } else if (diffValue2 == diffValue1) { // 解相同，留最小升序
                        List<Integer> weightList = new ArrayList<>();
                        for (MiguServer server : tmpServers){
                            weightList.add(server.getWeight());
                        }
                        Collections.sort(weightList);

                        for (int n = 0; n < weightList.size(); n++){
                            String sequence1 = new String();
                            String sequence2 = new String();
                            for (MiguServer server1 : tmpServers) {
                                if(server1.getNodeIdByWeight(weightList.get(n)) > 0) {
                                    for (Map.Entry<Integer, MiguTask> entry : server1.getTasks().entrySet()) {
                                        sequence1 += entry.getValue().getTaskId();
                                    }
                                }
                            }
                            for (MiguServer server2 : optServers) {
                                if(server2.getNodeIdByWeight(weightList.get(n)) > 0) {
                                    for (Map.Entry<Integer, MiguTask> entry : server2.getTasks().entrySet()) {
                                        sequence2 += entry.getValue().getTaskId();
                                    }
                                }
                            }
                            if (!sequence1.isEmpty() && !sequence2.isEmpty() && Integer.parseInt(sequence1) < Integer.parseInt(sequence2)) {
                                optServers = (List<MiguServer>) ((ArrayList<MiguServer>) tmpServers).clone();
                                break;
                            }
                        }
                    }
                }
            }

            currentTasks.add(currentTasks.get(0));
            currentTasks.remove(0);

            tmpServers.clear();
            for(Map.Entry<Integer,MiguServer> entry: miguServers.entrySet()) {
                MiguServer server = new MiguServer(entry.getValue().getNodeId());
                tmpServers.add(server);
            }
        }

        if(getDValue(optServers) > threshold) {
            return ReturnCodeKeys.E014;
        }

        List<Integer> sortServerNodeIds = new ArrayList<Integer>();
        for (MiguServer server : optServers) {
            sortServerNodeIds.add(server.getNodeId());
        }
        Collections.sort(sortServerNodeIds);
        Collections.sort(optServers, Comparator.comparing(MiguServer::getWeight));

        for (int i = 0; i < sortServerNodeIds.size();i++){
            optServers.get(i).setNodeId(sortServerNodeIds.get(i));
            miguServers.put(sortServerNodeIds.get(i),optServers.get(i));
        }

        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(tasks == null) {
            return ReturnCodeKeys.E016;
        } else {
            tasks.clear();
            for(Map.Entry<Integer,MiguServer> serverEntry: miguServers.entrySet()){
                for(Map.Entry<Integer,MiguTask> taskEntry : serverEntry.getValue().getTasks().entrySet()){
                    TaskInfo info = new TaskInfo();
                    info.setNodeId(serverEntry.getValue().getNodeId());
                    info.setTaskId(taskEntry.getValue().getTaskId());
                    tasks.add(info);
                }
            }

            Collections.sort(tasks, Comparator.comparing(TaskInfo::getTaskId));
            return ReturnCodeKeys.E015;
        }
    }

}
