/*
 * Copyright 2018 dc-square GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hivemq.extensions.heartbeat.rabc;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.auth.SimpleAuthenticator;
import com.hivemq.extension.sdk.api.auth.parameter.SimpleAuthInput;
import com.hivemq.extension.sdk.api.auth.parameter.SimpleAuthOutput;
import com.hivemq.extension.sdk.api.packets.connect.ConnackReasonCode;
import com.hivemq.extensions.heartbeat.plug.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public class FileAuthAuthenticator implements SimpleAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(FileAuthAuthenticator.class);


    @Override
    public void onConnect(@NotNull final SimpleAuthInput simpleAuthInput, @NotNull final SimpleAuthOutput simpleAuthOutput) {
        log.info("one client onConnect");
        final Optional<String> userNameOptional = simpleAuthInput.getConnectPacket().getUserName();
        final Optional<ByteBuffer> passwordOptional = simpleAuthInput.getConnectPacket().getPassword();
        final String clientId = simpleAuthInput.getClientInformation().getClientId();

        //check if username and password are present
        if (!userNameOptional.isPresent() || !passwordOptional.isPresent()) {
            //client is not authenticated
            log.error("userNameOptional 是否为空,{},passwordOptional 是否为空,{}",userNameOptional.isPresent(),passwordOptional.isPresent());
            simpleAuthOutput.failAuthentication(ConnackReasonCode.BAD_USER_NAME_OR_PASSWORD, "Authentication failed because username or password are missing");
            return;
        }

        String userName = userNameOptional.get();
        String password = Charset.forName("UTF-8").decode(passwordOptional.get()).toString();
        log.info("one client onConnect clientId:{},userName:{},password{}",clientId,userName,password);

        //prevent clientIds with MQTT wildcard characters
        if (clientId.contains("#") || clientId.contains("+")) {
            //client is not authenticated
            simpleAuthOutput.failAuthentication(ConnackReasonCode.CLIENT_IDENTIFIER_NOT_VALID, "The characters '#' and '+' are not allowed in the client identifier");
            return;
        }

        //prevent usernames with MQTT wildcard characters
        if (userName.contains("#") || userName.contains("+")) {
            //client is not authenticated
            simpleAuthOutput.failAuthentication(ConnackReasonCode.BAD_USER_NAME_OR_PASSWORD, "The characters '#' and '+' are not allowed in the username");
            return;
        }
       String pwd =  RedisUtils.getJedis().get("user:" + userName);
        if (password.equals(pwd)) {
            log.info("onConnect success:"+clientId);
            simpleAuthOutput.authenticateSuccessfully();
        } else {
            log.info("onConnect fail" +
                    "" +
                    "" +
                    ":"+clientId);
            simpleAuthOutput.failAuthentication();
        }


//        //check if we have any roles for username/password combination
//        final List<String> roles = credentialsValidator.getRoles(userName, passwordOptional.get());
//
//        if (roles == null || roles.isEmpty()) {
//            //username/password combination is unknown or has invalid roles
//            simpleAuthOutput.failAuthentication(ConnackReasonCode.NOT_AUTHORIZED, "Authentication failed because of invalid credentials");
//            return;
//        }
//
//        //username/password combination is valid and has roles, so we set the default permissions for this client
//        final List<TopicPermission> topicPermissions = credentialsValidator.getPermissions(clientId, userName, roles);
//        simpleAuthOutput.getDefaultPermissions().addAll(topicPermissions);
//        simpleAuthOutput.getDefaultPermissions().setDefaultBehaviour(DefaultAuthorizationBehaviour.DENY);

//        simpleAuthOutput.authenticateSuccessfully();
    }

}
