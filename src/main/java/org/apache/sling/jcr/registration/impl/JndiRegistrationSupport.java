/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sling.jcr.registration.impl;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Properties;

import javax.jcr.Repository;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.sling.jcr.registration.AbstractRegistrationSupport;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.propertytypes.ServiceDescription;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import org.osgi.service.log.LoggerFactory;
import org.osgi.service.log.Logger;

/**
 * The <code>JndiRegistrationSupport</code> extends the
 * {@link AbstractRegistrationSupport} class to register repositories with a
 * JNDI context whose provider URL and initial factory class name may be
 * configured.
 * <p>
 * Note: Currently, only these two properties are declared to be configurable,
 * in the future a mechanism should be devised to support declaration of more
 * properties.
 */
@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        name = "org.apache.sling.jcr.jackrabbit.server.JndiRegistrationSupport",
        reference = {
                @Reference(name = "Repository", policy = ReferencePolicy.DYNAMIC,
                        cardinality = ReferenceCardinality.MULTIPLE, service = Repository.class)
        }
)
@Designate(ocd = JndiRegistrationSupport.Configuration.class)
@ServiceDescription("JNDI Repository Registration")
public class JndiRegistrationSupport extends AbstractRegistrationSupport {

    private Context jndiContext;

    // ---------- Configuration ---------------------------------------------

    @ObjectClassDefinition(name = "Apache Sling JCR Repository JNDI Registrar",
            description = "The JNDI Registrar listens for embedded repositories " +
                    "to be registered as services and registers them in the JNDI context under the " +
                    "name specified in the \"name\" service property.")
    public @interface Configuration {

        @AttributeDefinition(
                name = "Initial Context Factory",
                description = "The fully qualified class name of the factory class that will create an initial context.")
        String java_naming_factory_initial() default "org.apache.jackrabbit.core.jndi.provider.DummyInitialContextFactory"; //NOSONAR

        @AttributeDefinition(
                name = "Provider URL",
                description = "An URL string for the service provider (e.g. ldap://somehost:389)")
        String java_naming_provider_url() default "http://sling.apache.org"; //NOSONAR

    }

    // ---------- SCR integration ---------------------------------------------

    @Override
    protected boolean doActivate() {
        @SuppressWarnings("unchecked")
        Dictionary<String, Object> props = this.getComponentContext().getProperties();
        Properties env = new Properties();
        for (Enumeration<String> pe = props.keys(); pe.hasMoreElements();) {
            String key = pe.nextElement();
            if (key.startsWith("java.naming.")) {
                env.setProperty(key, (String) props.get(key));
            }
        }

        try {
            // create the JNDI context for registration
            this.jndiContext = this.createInitialContext(env);
            logger.info("Using JNDI context {} to register repositories", this.jndiContext.getEnvironment());

            return true;
        } catch (NamingException ne) {
            logger.error("Problem setting up JNDI initial context, repositories will not be registered. Reason: {}", ne.getMessage());
        }

        // fallback to false
        return false;
    }

    @Override
    protected void doDeactivate() {
        if (this.jndiContext != null) {
            try {
                this.jndiContext.close();
            } catch (NamingException ne) {
                logger.info("Problem closing JNDI context", ne);
            }

            this.jndiContext = null;
        }
    }

    private Context createInitialContext(final Properties env) throws NamingException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<Context>) () -> {
                Thread currentThread = Thread.currentThread();
                ClassLoader old = currentThread.getContextClassLoader();
                currentThread.setContextClassLoader(JndiRegistrationSupport.this.getClass().getClassLoader());
                try {
                    return new InitialContext(env);
                } finally {
                    currentThread.setContextClassLoader(old);
                }
            });
        } catch (PrivilegedActionException pae) {
            // we now that this method only throws a NamingException
            throw (NamingException) pae.getCause();
        }
    }

    @Override
    protected Object bindRepository(String name, Repository repository) {

        if (this.jndiContext != null) {
            try {
                this.jndiContext.bind(name, repository);
                logger.info("Repository bound to JNDI as {}", name);
                return repository;
            } catch (NamingException ne) {
                logger.error("Failed to register repository {}", name, ne);
            }
        }

        // fall back to unregistered in case of failures or no context
        return null;
    }

    @Override
    protected void unbindRepository(String name, Object data) {
        if (this.jndiContext != null) {
            try {
                this.jndiContext.unbind(name);
                logger.info("Repository {} unbound from JNDI", name);
            } catch (NamingException ne) {
                logger.error("Problem unregistering repository {}", name, ne);
            }
        }
    }

    @Override
    @Reference(service = LoggerFactory.class)
    protected void bindLogger(Logger logger) {
        super.bindLogger(logger);
    }
}
