package com.migu.schedule.task;

public class MiguTask {
    private int nodeId;
    private int taskId;
    private int consumption;

    public MiguTask(){
        return;
    }

    public MiguTask(int taskId, int consumption){
        setTaskId(taskId);
        setConsumption(consumption);
        return;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getConsumption() {
        return consumption;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setConsumption(int consumption) {
        this.consumption = consumption;
    }
}
