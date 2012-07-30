package com.rails.mbta.commuterrail.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import com.rails.mbta.commuterrail.model.Line;
import com.rails.mbta.commuterrail.schedule.Calendar;
import com.rails.mbta.commuterrail.schedule.Route;
import com.rails.mbta.commuterrail.schedule.Station;
import com.rails.mbta.commuterrail.schedule.Stop;
import com.rails.mbta.commuterrail.schedule.StopTime;
import com.rails.mbta.commuterrail.schedule.Trip;

public class CommuterRailGTFSParser {
    public static void main(String[] args) {
        RouteFileReader routeFileReader = new RouteFileReader();
        routeFileReader.read("routes.txt");

        StationFileReader stationFileReader = new StationFileReader(routeFileReader.routesByid);
        stationFileReader.read("CommuterRailStationLineOrdering.csv");

        TripFileReader tripFileReader = new TripFileReader(routeFileReader.routesByid);
        tripFileReader.read("trips.txt");

        StopTimeFileReader stopTimeFileReader = new StopTimeFileReader(tripFileReader.tripById);
        stopTimeFileReader.read("stop_times.txt");

        /*
         * Order stop by stop sequence
         */
        for (Map.Entry<String, Route> route : routeFileReader.routesByid.entrySet()) {
            for (Trip trip : route.getValue().trips) {
                Collections.sort(trip.stopTimes, new Comparator<StopTime>() {
                    @Override
                    public int compare(StopTime o1, StopTime o2) {
                        return (Integer.valueOf(o1.stopSequence)).compareTo(Integer.valueOf(o2.stopSequence));
                    }
                });
            }
        }

        /*
         * Now order trips by their first stop time
         */
        for (Map.Entry<String, Route> route : routeFileReader.routesByid.entrySet()) {
            Collections.sort(route.getValue().trips, new Comparator<Trip>() {
                @Override
                public int compare(Trip o1, Trip o2) {
                    return o1.stopTimes.get(0).departureTime.compareTo(o2.stopTimes.get(0).departureTime);
                }
            });
        }

        StopFileReader stopFileReader = new StopFileReader(stopTimeFileReader.stopIds, stopTimeFileReader.stopTimes);
        stopFileReader.read("stops.txt");

        CalendarFileReader calendarFileReader = new CalendarFileReader(tripFileReader.serviceIds);
        calendarFileReader.read("calendar.txt");

        // Add calendar information into each trip
        for (Trip trip : tripFileReader.tripById.values()) {
            trip.service = calendarFileReader.calendersById.get(trip.serviceId);
        }

        // output 1 serialized file per line to make schedule load times faster
        try {
            for (Map.Entry<String, Route> route : routeFileReader.routesByid.entrySet()) {
                Line line = Line.valueOfName(route.getValue().routeLongName.replace(" Line", ""));
                FileOutputStream out = new FileOutputStream("CR-" + line.getLineNumber() + "-data.ser");
                ObjectOutputStream oo = new ObjectOutputStream(out);
                oo.writeObject(route.getValue());
                oo.close();

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class RouteFileReader extends GTFSFileReader {
        public Map<String, Route> routesByid = new HashMap<String, Route>();

        @Override
        protected boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex) {
            if (lineValues[headerIndex.get("route_type")].equals("2")) {
                Route route = new Route();
                route.routeId = lineValues[headerIndex.get("route_id")];
                route.routeLongName = lineValues[headerIndex.get("route_long_name")];
                routesByid.put(route.routeId, route);

                return true;
            }
            return false;
        }
    }

    private static class CalendarFileReader extends GTFSFileReader {
        private Set<String> serviceIds;
        public Map<String, Calendar> calendersById = new HashMap<String, Calendar>();

        public CalendarFileReader(Set<String> serviceIds) {
            this.serviceIds = serviceIds;
        }

        @Override
        protected boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex) {
            if (serviceIds.contains(lineValues[headerIndex.get("service_id")])) {
                Calendar calendar = new Calendar();
                calendar.serviceId = lineValues[headerIndex.get("service_id")];
                calendar.serviceDays[1] = lineValues[headerIndex.get("monday")].equals("0") ? false : true;
                calendar.serviceDays[2] = lineValues[headerIndex.get("tuesday")].equals("0") ? false : true;
                calendar.serviceDays[3] = lineValues[headerIndex.get("wednesday")].equals("0") ? false : true;
                calendar.serviceDays[4] = lineValues[headerIndex.get("thursday")].equals("0") ? false : true;
                calendar.serviceDays[5] = lineValues[headerIndex.get("friday")].equals("0") ? false : true;
                calendar.serviceDays[6] = lineValues[headerIndex.get("saturday")].equals("0") ? false : true;
                calendar.serviceDays[7] = lineValues[headerIndex.get("sunday")].equals("0") ? false : true;
                calendar.startDate = getLocalDate(lineValues[headerIndex.get("start_date")]);
                calendar.endDate = getLocalDate(lineValues[headerIndex.get("end_date")]);

                calendersById.put(calendar.serviceId, calendar);

                return true;
            }
            return false;
        }
    }

    private static class StopFileReader extends GTFSFileReader {
        private List<StopTime> stopTimes;
        private Set<String> stopIds;

        public StopFileReader(Set<String> stopIds, List<StopTime> stopTimes) {
            this.stopIds = stopIds;
            this.stopTimes = stopTimes;
        }

        @Override
        protected boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex) {
            if (stopIds.contains(lineValues[headerIndex.get("stop_id")])) {
                Stop stop = new Stop();
                stop.stopId = lineValues[headerIndex.get("stop_id")];
                stop.stopName = lineValues[headerIndex.get("stop_name")];

                for (StopTime stopTime : stopTimes) {
                    if (stopTime.stopId.equals(stop.stopId)) {
                        stopTime.stop = stop;
                    }
                }

                return true;
            }
            return false;
        }
    }

    private static class StopTimeFileReader extends GTFSFileReader {
        private Map<String, Trip> tripById;
        public List<StopTime> stopTimes = new ArrayList<StopTime>();
        public Set<String> stopIds = new HashSet<String>();

        public StopTimeFileReader(Map<String, Trip> tripById) {
            this.tripById = tripById;
        }

        @Override
        protected boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex) {
            if (tripById.containsKey(lineValues[headerIndex.get("trip_id")])) {
                StopTime stopTime = new StopTime();
                stopTime.arrivalTime = getLocalTime(lineValues[headerIndex.get("arrival_time")]);
                stopTime.departureTime = getLocalTime(lineValues[headerIndex.get("departure_time")]);
                stopTime.stopId = lineValues[headerIndex.get("stop_id")];
                stopTime.stopSequence = Integer.parseInt(lineValues[headerIndex.get("stop_sequence")]);

                Trip tripForStopTime = tripById.get(lineValues[headerIndex.get("trip_id")]);
                stopTime.trip = tripForStopTime;
                tripForStopTime.stopTimes.add(stopTime);

                stopTimes.add(stopTime);
                stopIds.add(stopTime.stopId);
                return true;
            }
            return false;
        }
    }

    private static LocalTime getLocalTime(String time) {
        String[] timeSplit = time.split(":");

        // Format dictates that stops that started late one day can have times
        // >= 24 hours to indicate the same day.
        int hour = (Integer.valueOf(timeSplit[0]));
        hour = hour > 23 ? hour - 24 : hour;

        return new LocalTime(hour, Integer.valueOf(timeSplit[1]), Integer.valueOf(timeSplit[2]));
    }

    private static LocalDate getLocalDate(String date) {
        return new LocalDate(Integer.valueOf(date.substring(0, 4)), Integer.valueOf(date.substring(4, 6)),
                Integer.valueOf(date.substring(6, 8)));
    }

    private static class StationFileReader extends GTFSFileReader {
        private Map<String, Route> routesById;

        public StationFileReader(Map<String, Route> routesByid) {
            this.routesById = routesByid;
        }

        @Override
        protected boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex) {
            if (lineValues.length == 0) {
                return false;
            }
            if (lineValues[headerIndex.get("direction_id")].equals("0")) {
                return false;
            }

            for (Map.Entry<String, Route> route : routesById.entrySet()) {
                if (!lineValues[headerIndex.get("route_long_name")].equals(route.getValue().routeLongName)) {
                    continue;
                }

                Station station = new Station();
                station.routeLongName = lineValues[headerIndex.get("route_long_name")];
                station.branch = lineValues[headerIndex.get("Branch")];
                station.directionId = Integer.valueOf(lineValues[headerIndex.get("direction_id")]);
                station.stopId = lineValues[headerIndex.get("stop_id")];
                station.stopLat = Double.valueOf(lineValues[headerIndex.get("stop_lat")]);
                station.stopLon = Double.valueOf(lineValues[headerIndex.get("stop_lon")]);
                station.stopSequence = Integer.valueOf(lineValues[headerIndex.get("stop_sequence")]);

                route.getValue().stations.add(station);

                return true;
            }
            return false;
        }
    }

    private static class TripFileReader extends GTFSFileReader {
        private Map<String, Route> routesById;
        public Map<String, Trip> tripById = new HashMap<String, Trip>();
        public Set<String> serviceIds = new HashSet<String>();

        public TripFileReader(Map<String, Route> routesByid) {
            this.routesById = routesByid;
        }

        @Override
        protected boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex) {
            if (routesById.containsKey(lineValues[headerIndex.get("route_id")])) {
                Trip trip = new Trip();
                trip.tripId = lineValues[headerIndex.get("trip_id")];
                trip.serviceId = lineValues[headerIndex.get("service_id")];
                trip.directionId = Integer.valueOf(lineValues[headerIndex.get("direction_id")]);
                trip.tripHeadsign = lineValues[headerIndex.get("trip_headsign")];

                Route routeForTrip = routesById.get(lineValues[headerIndex.get("route_id")]);
                trip.route = routeForTrip;
                routeForTrip.trips.add(trip);

                tripById.put(trip.tripId, trip);
                serviceIds.add(trip.serviceId);

                return true;
            }
            return false;
        }
    }

    private static abstract class GTFSFileReader {

        public void read(String inFile) {
            try {
                InputStream is = getClass().getResourceAsStream("/" + inFile);
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));

                File outputRoute = new File("CR-" + inFile);
                BufferedWriter writer = new BufferedWriter(new FileWriter(outputRoute));

                // The 1st line should be header information
                String headers[] = reader.readLine().split(",");

                Map<String, Integer> headerIndex = new HashMap<String, Integer>();
                for (int i = 0; i < headers.length; ++i) {
                    headerIndex.put(headers[i].replace("\"", ""), i);
                }

                String line = null;
                while ((line = reader.readLine()) != null) {
                    line = line.replace("\"", "");
                    String[] lineValues = line.split(",");

                    if (parseLine(lineValues, headerIndex)) {
                        writer.write(line + "\n");
                    }
                }

                writer.close();
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        protected abstract boolean parseLine(String[] lineValues, Map<String, Integer> headerIndex);
    }
}
