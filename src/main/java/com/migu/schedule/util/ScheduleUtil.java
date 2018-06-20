package com.migu.schedule.util;

import com.migu.schedule.server.MiguServer;

import java.util.List;

public class ScheduleUtil {

    public static int getDValue(List<MiguServer> servers) {

        int maxValue = servers.get(0).getTotalConsumption();
        int minValue = maxValue;
        for(MiguServer server : servers){
            maxValue = Math.max(maxValue,server.getTotalConsumption());
            minValue = Math.min(minValue,server.getTotalConsumption());
        }
        return (maxValue - minValue);
    }

}
