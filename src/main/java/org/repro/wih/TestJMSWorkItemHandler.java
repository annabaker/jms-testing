/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.repro.wih;

import java.util.Collections;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.kie.server.api.commands.CommandScript;
import org.kie.server.api.commands.DescriptorCommand;
import org.kie.server.api.jms.JMSConstants;
import org.kie.server.api.marshalling.Marshaller;
import org.kie.server.api.marshalling.MarshallerFactory;
import org.kie.server.api.marshalling.MarshallingFormat;
import org.kie.server.api.model.KieServerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestJMSWorkItemHandler
        implements
        WorkItemHandler {
	
    private static final Logger logger = LoggerFactory.getLogger( TestJMSWorkItemHandler.class );
	
    private static int numReqMsgs = 10000;
    private static WorkItem workItem = null;
    
	// connection factory 
	private static String XAConnectionFactory = "java:/JmsXA";
	
	// kie server user
	private static String servUser = "admin";
	private static String servPass = "admin";
	
	// workbench user
	private static String wbUser = "test";
	private static String wbPass = "test";
	
	// session creation params
	private static boolean tx = true;
	private int ack = Session.SESSION_TRANSACTED;

    public void abortWorkItem(WorkItem arg0,
                              WorkItemManager arg1) {
    	// do nothing
    }

    
    public void executeWorkItem(WorkItem item,
                                WorkItemManager manager) {
        workItem = item;
		
		logger.info("**** Inside TestJMSWorkItemHandler, suspending.... ****");
		
			try {
				// queue setup
				InitialContext initialContext = new InitialContext();
				Queue requestQueue = (Queue) initialContext.lookup("queue/KIE.SERVER.REQUEST");
				
				QueueConnectionFactory connectionFactory = (QueueConnectionFactory) initialContext.lookup(XAConnectionFactory);
				logger.info("WIH is using connectionFactory {}", connectionFactory.getClass());
				
				QueueConnection connection = (QueueConnection) connectionFactory.createConnection(servUser,servPass);
				
				// if we use the global pool, connection factory will take care of first parameter
				Session session = connection.createQueueSession(tx, ack);
				logger.debug("WIH session tx, ack are {}", session.getTransacted(), session.getAcknowledgeMode());

				MessageProducer producer = session.createProducer(requestQueue);		
				
				// process-relevant information needed to marshall the service commands
				String containerId = "org.kie.testing:new-jms-test:1.0.0-SNAPSHOT";
				Number processInstanceId = workItem.getProcessInstanceId();
				Number workItemId = workItem.getId();
												
				// marshall commands to be used
				String signalsCommand = getCommandString(containerId, processInstanceId, workItemId, "getAvailableSignals");		
				String completeCommand = getCommandString(containerId, processInstanceId, workItemId, "completeWorkItem");

				// jms message setup for getAvailableSignals commands
				TextMessage message = null;
				message = prepMessage(session, message, signalsCommand, containerId);

				int i = 0;
	            for (i = 0 ; i < numReqMsgs ; i++) {
	                message.setJMSCorrelationID(UUID.randomUUID().toString());
	                producer.send(message);
	            }
	            
	            if ( i == numReqMsgs) {
					logger.info("WIH has completed sending {} getAvailableSignals commands:", numReqMsgs );
	            }
	            
	            // jms message setup for completeWorkItem command
	            message = prepMessage(session, message, completeCommand, containerId);
				producer.send(message);
				logger.info("WIH has completed sending {} completeWorkItem command.");
				
				producer.close();
				session.close();
				connection.close();

				// TODO Auto-generated catch block
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NamingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }

    public static WorkItem getWorkItem() {
        return workItem;
    }
    
    private static TextMessage prepMessage(Session session, TextMessage message, String commandString, String containerId) throws JMSException {
    	message = session.createTextMessage(commandString);
    	message.setJMSCorrelationID(UUID.randomUUID().toString());
		message.setStringProperty(JMSConstants.USER_PROPERTY_NAME, wbUser);
		message.setStringProperty(JMSConstants.PASSWRD_PROPERTY_NAME, wbPass);
		message.setStringProperty(JMSConstants.TARGET_CAPABILITY_PROPERTY_NAME, "BPM");
		message.setStringProperty(JMSConstants.CONTAINER_ID_PROPERTY_NAME, containerId);
		message.setIntProperty( JMSConstants.SERIALIZATION_FORMAT_PROPERTY_NAME, MarshallingFormat.JAXB.getId());
		message.setIntProperty( JMSConstants.INTERACTION_PATTERN_PROPERTY_NAME, JMSConstants.REQUEST_REPLY_PATTERN);
		return message;
    }
    
	private static String getCommandString(String containerId, Number processInstanceId, Number workItemId, String processService) {
		Marshaller marshaller = MarshallerFactory.getMarshaller( Collections.<Class<?>> emptySet() , MarshallingFormat.JAXB, Thread.currentThread()
				.getContextClassLoader() );
		
		CommandScript script = new CommandScript();
		
		switch (processService) {
			case "completeWorkItem":
				script = new CommandScript( Collections.singletonList(
						(KieServerCommand) new DescriptorCommand( "ProcessService", processService, new Object[]{containerId, processInstanceId, workItemId, "", marshaller.getFormat().getType()}) ) );
				break;
			case "getAvailableSignals":
				script = new CommandScript( Collections.singletonList(
						(KieServerCommand) new DescriptorCommand( "ProcessService", processService, new Object[]{containerId, processInstanceId, marshaller.getFormat().getType()}) ) );
				break;
			default:
				throw new IllegalArgumentException("No marshalling implementation in getCommandString() exists for " + processService);
		}
		
		return marshaller.marshall(script);		
	} 

}
