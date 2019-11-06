package com.hivemq.extensions.heartbeat.publish;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.annotations.Nullable;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.general.UserProperties;
import com.hivemq.extension.sdk.api.packets.general.UserProperty;
import com.hivemq.extension.sdk.api.packets.publish.PayloadFormatIndicator;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import com.hivemq.extensions.heartbeat.plug.Producer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @created with IntelliJ IDEA.
 * @author: heaven
 * @date: 2019/11/5
 * @time: 14:43
 * @description:
 **/
public class PublishInboundInterceptorImpl implements PublishInboundInterceptor {

    private static final Logger log = LoggerFactory.getLogger(PublishInboundInterceptorImpl.class);

    @Override
    public void onInboundPublish(@NotNull PublishInboundInput publishInboundInput,@NotNull PublishInboundOutput publishInboundOutput) {
        final String clientID = publishInboundInput.getClientInformation().getClientId();
        PublishPacket publishPacket = publishInboundInput.getPublishPacket();
        String topic = publishPacket.getTopic();



        final int qos = publishPacket.getQos().getQosNumber();
        final boolean retained = publishPacket.getRetain();
        final Optional<ByteBuffer> payload = publishPacket.getPayload();
        final Optional<String> contentType = publishPacket.getContentType();
        final String payloadAsString = getStringFromByteBuffer(payload.orElse(null));




        final String correlationDataString = getStringFromByteBuffer(publishPacket.getCorrelationData().orElse(null));
        final Optional<String> responseTopic = publishPacket.getResponseTopic();
        final Optional<Long> messageExpiryInterval = publishPacket.getMessageExpiryInterval();
        final boolean dupFlag = publishPacket.getDupFlag();
        final Optional<PayloadFormatIndicator> payloadFormatIndicator = publishPacket.getPayloadFormatIndicator();
        final List<Integer> subscriptionIdentifiers = publishPacket.getSubscriptionIdentifiers();

        final String userPropertiesAsString = getUserPropertiesAsString(publishPacket.getUserProperties());

        log.info(String.format("Payload: '%s'," +
                        " QoS: '%s'," +
                        " Retained: '%s'," +
                        " Message Expiry Interval: '%s'," +
                        " Duplicate Delivery: '%s'," +
                        " Correlation Data: '%s'," +
                        " Response Topic: '%s'," +
                        " Content Type: '%s'," +
                        " Payload Format Indicator: '%s'," +
                        " Subscription Identifiers: '%s'," +
                        " %s", //user properties
                payloadAsString, qos, retained, messageExpiryInterval.orElse(null), dupFlag, correlationDataString,
                responseTopic.orElse(null), contentType.orElse(null), payloadFormatIndicator.orElse(null),
                subscriptionIdentifiers, userPropertiesAsString));
        log.info("topic:{},payloadAsString:{}",topic,payloadAsString);
        try {
            Producer.send("device_7253724c55d0357142721288","ON MQTT:"+payloadAsString);
        } catch (MQClientException e) {
            log.error("MQClientException exception:",e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            log.error("InterruptedException exception:",e);
            e.printStackTrace();
        }


    }


    @Nullable
    private static String getStringFromByteBuffer(final @Nullable ByteBuffer buffer) {
        if(buffer == null){
            return null;
        }
        final byte[] bytes = new byte[buffer.remaining()];
        for (int i = 0; i < buffer.remaining(); i++) {
            bytes[i] = buffer.get(i);
        }
        return new String(bytes, UTF_8);
    }

    @NotNull
    private static String getUserPropertiesAsString(final @Nullable UserProperties userProperties) {
        if(userProperties == null){
            return "User Properties: 'null'";
        }
        final List<UserProperty> userPropertyList = userProperties.asList();
        if(userPropertyList.size() == 0){
            return "User Properties: 'null'";
        }
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < userPropertyList.size(); i++) {
            final UserProperty userProperty = userPropertyList.get(i);
            if (i == 0) {
                stringBuilder.append("User Properties: ");
            } else {
                stringBuilder.append(", ");
            }
            stringBuilder.append("[Name: '").append(userProperty.getName());
            stringBuilder.append("', Value: '").append(userProperty.getValue()).append("']");
        }
        return stringBuilder.toString();
    }
}
