
/*
 * Copyright 2019 dc-square GmbH
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

package com.hivemq.extensions.heartbeat;

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.events.EventRegistry;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.intializer.ClientInitializer;
import com.hivemq.extensions.heartbeat.configuration.ExtensionConfiguration;
import com.hivemq.extensions.heartbeat.publish.PublishInboundInterceptorImpl;
import com.hivemq.extensions.heartbeat.rabc.FileAuthenticatorProvider;
import com.hivemq.extensions.heartbeat.service.HTTPService;
import com.hivemq.extensions.heartbeat.servlet.HiveMQHeartbeatServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Main class for HiveMQ Heartbeat extension
 *
 * If HiveMQ is starting and starts this extension:
 *  The settings were read from configuration file
 *  the HTTPS Server will be started
 *  and the HeartbeatServlet instantiated.
 * If HiveMQ stops - stops the HTTP Server
 *
 * @Author Anja Helmbrecht-Schaar
 *
 */
public class HeartbeatMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(HeartbeatMain.class);
    private static @NotNull HTTPService httpService;

    @Override
    public final void extensionStart(final @NotNull ExtensionStartInput extensionStartInput,
                                     final @NotNull ExtensionStartOutput extensionStartOutput) {
        try {

            addClientLifecycleEventListener();

            final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
            log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

        } catch (Exception e) {
            log.error("addClientLifecycleEventListener thrown at extension start: ", e);
        }

        try {

            addAuthenticatorProvider();

        } catch (Exception e) {
            log.error("addAuthenticatorProvider thrown at extension start: ", e);
        }


        try {

            addClientInitializer();

        } catch (Exception e) {
            log.error("addAuthenticatorProvider thrown at extension start: ", e);
        }


        try {

            addHttpService(extensionStartInput);


        } catch (Exception e) {
            log.error("addHttpService thrown at extension start: ", e);
        }

    }

    @Override
    public final void extensionStop(final @NotNull ExtensionStopInput extensionStopInput, final @NotNull ExtensionStopOutput extensionStopOutput) {
        if( httpService != null ) {
            httpService.stopHTTPServer();
        }

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());
    }

    @NotNull
    private void startRestService(final @NotNull ExtensionConfiguration extensionConfiguration) {
        httpService = new HTTPService(extensionConfiguration.getHeartbeatConfig(), new HiveMQHeartbeatServlet());
        httpService.startHttpServer();
    }

    /**
     * 添加启动日志
     */
    private void addClientLifecycleEventListener() {

        final EventRegistry eventRegistry = Services.eventRegistry();

        final HelloWorldListener helloWorldListener = new HelloWorldListener();

        eventRegistry.setClientLifecycleEventListener(input -> helloWorldListener);

    }

    /**
     * 客户端连接权限认证
     */
    private void addAuthenticatorProvider() {

        Services.securityRegistry().setAuthenticatorProvider(new FileAuthenticatorProvider());
        log.info("Started 客户端连接权限认证");

    }


    /**
     * 消息写入其他中间件
     */
    private void addClientInitializer() {

        PublishInboundInterceptor publishInboundInterceptor = new PublishInboundInterceptorImpl();
        final ClientInitializer initializer = (initializerInput, clientContext) -> {
            clientContext.addPublishInboundInterceptor(publishInboundInterceptor);
        };

        Services.initializerRegistry().setClientInitializer(initializer);
        log.info("Started 消息写入其他中间件");

    }

    /**
     * 启动HTTPservice
     */
    private void addHttpService(ExtensionStartInput extensionStartInput) {

        final @NotNull File extensionHomeFolder = extensionStartInput.getExtensionInformation().getExtensionHomeFolder();
        final @NotNull ExtensionConfiguration extensionConfiguration = new ExtensionConfiguration(extensionHomeFolder);

        httpService = new HTTPService(extensionConfiguration.getHeartbeatConfig(), new HiveMQHeartbeatServlet());
        httpService.startHttpServer();
        log.info("Started 开启端口心跳监测");

    }

}
