/**
 * Copyright 2016 Evokly S.A.
 * See LICENSE file for License
 **/

package com.evokly.kafka.connect.mqtt;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MqttSourceTaskTest {
    private MqttSourceConnector mConnector;
    Map<String, String> mSourceProperties;

    private MqttSourceTask mTask;
    private Map<String, String> mEmptyConfig = new HashMap<String, String>();

    /**
     * Several tests need similar objects created before they can run.
     */
    @Before
    public void beforeEach() {
        mConnector = new MqttSourceConnector();

        mSourceProperties = new HashMap<>();

        mSourceProperties.put(MqttSourceConstant.KAFKA_TOPIC, "mqtt");

        mSourceProperties.put(MqttSourceConstant.MQTT_CLEAN_SESSION, "true");
        mSourceProperties.put(MqttSourceConstant.MQTT_CLIENT_ID, "TesetClientId");
        mSourceProperties.put(MqttSourceConstant.MQTT_CONNECTION_TIMEOUT, "15");
        mSourceProperties.put(MqttSourceConstant.MQTT_KEEP_ALIVE_INTERVAL, "30");
        mSourceProperties.put(MqttSourceConstant.MQTT_QUALITY_OF_SERVICE, "2");
        mSourceProperties.put(MqttSourceConstant.MQTT_SERVER_URIS, "tcp://mqtt.xuanma.tech:1883");
        mSourceProperties.put(MqttSourceConstant.MQTT_TOPIC, "test");
        mConnector.start(mSourceProperties);
        List<Map<String, String>> taskConfigs = mConnector.taskConfigs(1);

        assertEquals(taskConfigs.size(), 1);

        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.KAFKA_TOPIC), "mqtt");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_CLEAN_SESSION), "true");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_CLIENT_ID), "TesetClientId");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_CONNECTION_TIMEOUT), "15");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_KEEP_ALIVE_INTERVAL), "30");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_QUALITY_OF_SERVICE), "2");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_SERVER_URIS),
                "tcp://mqtt.xuanma.tech:1883");
        assertEquals(taskConfigs.get(0).get(MqttSourceConstant.MQTT_TOPIC), "test");

        mTask = new MqttSourceTask();
        //mTask.start(mEmptyConfig);
        mTask.start(taskConfigs.get(0));
    }

    @Test
    public void testPoll() throws Exception {
        // empty queue
        assertEquals(mTask.mQueue.size(), 0);

        // add dummy message to queue
        MqttMessage mqttMessage = new MqttMessage();
        mqttMessage.setPayload("qdqwdqwdqwd".getBytes(StandardCharsets.UTF_8));
        mTask.messageArrived("mqtt", mqttMessage);

        // check message to be process
        assertEquals(mTask.mQueue.size(), 1);

        // generate and validate SourceRecord
        List<SourceRecord> sourceRecords = mTask.poll();

        assertEquals(sourceRecords.size(), 1);
        assertEquals(sourceRecords.get(0).key(), "mqtt");

        System.out.println(new String((byte[]) sourceRecords.get(0).value(), "UTF-8"));

//        assertEquals(new String((byte[]) sourceRecords.get(0).value(), "UTF-8"), 1);

        // empty queue
        assertEquals(mTask.mQueue.size(), 0);
    }

    @Test
    public void testPoll2() throws Exception {
        MqttMessage mqttMessage = new MqttMessage();
        while (true) {
            mTask.messageArrived("mqtt",mqttMessage);
            // generate and validate SourceRecord
            List<SourceRecord> sourceRecords = mTask.poll();

            System.out.println(new String((byte[]) sourceRecords.get(0).value(), "UTF-8"));
        }
    }
}
