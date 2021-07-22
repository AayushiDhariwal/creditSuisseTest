package com.test.creditSuisse;

import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EventLogMapper {
    public static void main(String args[]) throws SQLException, ClassNotFoundException {

        if (args.length == 0 || args.length > 1) {
            System.out.println("Error: FileName is not passed or more than 1 file is passed");
            System.exit(0);
        }
        File f = new File(args[0]);
        String fileName = f.getAbsolutePath();
        System.out.println("File name passed is "+fileName);
        List<String> completeList = new ArrayList<>();
        Map<String, Map<String, Long>> data = new HashMap<>();
        DriverManagerConnection db = new DriverManagerConnection();
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            //Initialize Database
            db.initialize("LOGDETAILS");

            completeList = stream.collect(Collectors.toList());
            for (String s : completeList) {
                LogDetails startLogData = new Gson().fromJson(s, LogDetails.class);
                Map<String, Long> timeData = new HashMap<>();
                if (!data.containsKey(startLogData.getId())) {
                    timeData.put(startLogData.getState(), startLogData.getTimestamp());
                    data.put(startLogData.getId(), timeData);
                } else {
                    data.get(startLogData.getId()).put(startLogData.getState(), startLogData.getTimestamp());
                    long finishTimeStamp = data.get(startLogData.getId()).get("FINISHED").longValue();
                    long startTimeStamp = data.get(startLogData.getId()).get("STARTED").longValue();
                    long timeTaken = Math.subtractExact(finishTimeStamp,startTimeStamp);
                    boolean alert = false;
                    if (timeTaken > 4) {
                        alert = true;
                        System.out.println("Log Duration is higher than 4ms for id - " +  startLogData.getId() +" and log duration is - "+timeTaken+"ms");
                    }
                    //insert data in database
                    db.executeInsert( startLogData.getId(),timeTaken,startLogData.getType(),startLogData.getHost(),alert);
                }
            }
        }
        catch (NoSuchFileException e){
            System.out.println("File does not exists at mentioned path or fileName is incorrect. Please check again and retry.");
            System.exit(0);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            db.closeConnection();
        }
    }
}
