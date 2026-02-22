package com.sparkpractice.dse.services;

import com.opencagedata.jopencage.JOpenCageGeocoder;
import com.opencagedata.jopencage.model.JOpenCageForwardRequest;
import com.opencagedata.jopencage.model.JOpenCageLatLng;
import com.opencagedata.jopencage.model.JOpenCageResponse;

import java.util.*;


public class GeoCodesHandler {

    private final JOpenCageGeocoder jOpenCageGeocoder;

    public GeoCodesHandler(String OPEN_CAGE_API_KEY) {
        jOpenCageGeocoder = new JOpenCageGeocoder(OPEN_CAGE_API_KEY);
    }

    public Map<String, Optional<JOpenCageLatLng>> getBatchCoordinatesBasedOnAddressList(List<String> listOfAddresses) {
        Map<String, Optional<JOpenCageLatLng>> mapOfCoordinates = new HashMap<>();
        for (String address : listOfAddresses) {
            JOpenCageForwardRequest request = new JOpenCageForwardRequest(address);
            request.setLimit(1);
            request.setNoAnnotations(true);
            JOpenCageResponse response = jOpenCageGeocoder.forward(request);

            if (response != null && response.getResults() != null && !response.getResults().isEmpty()) {
                JOpenCageLatLng coordinates = response.getResults().get(0).getGeometry();
                mapOfCoordinates.put(address, Optional.of(coordinates));
                System.out.println(coordinates.getLat().toString() + "," + coordinates.getLng().toString());
            } else {
                mapOfCoordinates.put(address, Optional.empty());
                System.out.println("Unable to geocode input address: " + address);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return mapOfCoordinates;
    }
}
