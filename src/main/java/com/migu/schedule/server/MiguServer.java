package com.migu.schedule.server;

import com.migu.schedule.task.MiguTask;

import java.util.HashMap;
import java.util.Map;

public class MiguServer {
    private int nodeId;
    private int totalConsumption;
    private int weight;
    private HashMap<Integer,MiguTask> tasks = new HashMap<Integer,MiguTask>();

    public MiguServer(int nodeId){
        setNodeId(nodeId);
        return;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getNodeIdByWeight(int weight) {
        if(this.weight == weight) {
            return nodeId;
        } else {
            return -1;
        }
    }

    public int getNodeIdByTotalConsumption(int totalConsumption) {
        if(this.totalConsumption == totalConsumption) {
            return nodeId;
        } else {
            return -1;
        }
    }

    public void setTotalConsumption(int totalConsumption) {
        this.totalConsumption = totalConsumption;
    }

    public int getTotalConsumption() {
        return totalConsumption;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

    public void setTasks(HashMap<Integer, MiguTask> tasks) {
        this.tasks = tasks;
    }

    public HashMap<Integer, MiguTask> getTasks() {
        return tasks;
    }

    public MiguTask getTask(int taskId) {
        if (this.tasks.containsKey(taskId)) {
            return this.tasks.get(taskId);
        } else {
            return null;
        }
    }

    public void setTask(MiguTask task)
    {
        this.tasks.put(task.getTaskId(),task);
        task.setNodeId(this.nodeId);
        totalConsumption += task.getConsumption();

        int minTaskId;
        if(weight > 0) {
            minTaskId = Math.min(task.getTaskId(),weight%10);
        } else {
            minTaskId = task.getTaskId();
        }
        weight = totalConsumption * 100 + tasks.size() * 10 + minTaskId;
    }

    public void removeTask(MiguTask task)
    {
        if (this.tasks.containsKey(task.getTaskId())) {
            this.tasks.remove(task.getTaskId());
            task.setNodeId(-1);
            totalConsumption -= task.getConsumption();
            int minTaskId = 1000;
            for(Map.Entry<Integer,MiguTask> entry: this.tasks.entrySet()) {
                minTaskId = Math.min(minTaskId, entry.getValue().getTaskId());
            }
            weight = totalConsumption * 100 + tasks.size() * 10 + minTaskId;
        }
    }
}
