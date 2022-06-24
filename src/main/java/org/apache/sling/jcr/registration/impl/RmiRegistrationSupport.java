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

import java.io.IOException;
import java.net.InetAddress;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import javax.jcr.Repository;

import org.apache.jackrabbit.rmi.server.RemoteAdapterFactory;
import org.apache.jackrabbit.rmi.server.ServerAdapterFactory;
import org.apache.sling.jcr.registration.AbstractRegistrationSupport;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.propertytypes.ServiceDescription;
import org.osgi.service.log.Logger;
import org.osgi.service.log.LoggerFactory;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * The <code>RmiRegistrationSupport</code> extends the
 * {@link AbstractRegistrationSupport} class to register repositories with an
 * RMI registry whose provider localhost port may be configured.
 * <p>
 * Note: Currently only registries in this Java VM are supported. In the future
 * support for external registries may be added.
 */

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        name = "org.apache.sling.jcr.jackrabbit.server.RmiRegistrationSupport",
        reference = {
                @Reference(name = "Repository", policy = ReferencePolicy.DYNAMIC,
                        cardinality = ReferenceCardinality.MULTIPLE, service = Repository.class)
        }
)
@Designate(ocd = RmiRegistrationSupport.Configuration.class)
@ServiceDescription("RMI based Repository Registration")
public class RmiRegistrationSupport extends AbstractRegistrationSupport {

    public static final String PROP_REGISTRY_PORT = "port";

    private int registryPort;

    /** The private RMI registry, only defined if possible */
    private Registry registry;

    private boolean registryIsPrivate;

    // ---------- Configuration ---------------------------------------------

    @ObjectClassDefinition(name = "Apache Sling JCR Repository RMI Registrar",
            description = "The RMI Registrar listens for embedded repositories " +
                    " to be registered as services and registers them in an RMI registry under the " +
                    " name specified in the \"name\" service property.")
    public @interface Configuration {

        @AttributeDefinition(
                name = "Port Number",
                description = "Port number of the RMI registry to use. The RMI Registrar first tries to " +
                        "create a private RMI registry at this port. If this fails, an existing registry " +
                        "is tried to connect at this port on local host. If this number is negative, " +
                        "the RMI registrar is disabled. If this number is higher than 65535, an error " +
                        "message is logged and the RMI Registrar is also  disabled. If this number is " +
                        "zero, the system default RMI Registry port 1099 is used.")
        int port() default 1099;
    }

    // ---------- SCR intergration ---------------------------------------------

    /**
     * Read the registry port from the configuration properties. If the value is
     * invalid (higher than 65525), the RMI registry is disabled. Likewise the
     * registry is disabled, if the port property is negative. If the port is
     * zero or not a number, the default port (1099) is assumed.
     */
    @Override
    protected boolean doActivate() {

        Object portProp = this.getComponentContext().getProperties().get(
            PROP_REGISTRY_PORT);
        if (portProp instanceof Number) {
            this.registryPort = ((Number) portProp).intValue();
        } else {
            try {
                this.registryPort = Integer.parseInt(String.valueOf(portProp));
            } catch (NumberFormatException nfe) {
                this.registryPort = 0;
            }
        }

        // ensure correct value
        if (this.registryPort < 0) {
            logger.warn("RMI registry disabled (no or negative RMI port configured)");
            return false;
        } else if (this.registryPort == 0) {
            this.registryPort = Registry.REGISTRY_PORT;
        } else if (this.registryPort == 0 || this.registryPort > 0xffff) {
            logger.warn("Illegal RMI registry port number {} disabling RMI registry", this.registryPort);
            return false;
        }

        logger.info("Using RMI Registry port {}", this.registryPort);
        return true;
    }

    /**
     * If a private registry has been acquired this method unexports the
     * registry object to free the RMI registry OID for later use.
     */
    @Override
    protected void doDeactivate() {
        // if we have a private RMI registry, unexport it here to free
        // the RMI registry OID
        if (this.registry != null && this.registryIsPrivate) {
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
                logger.info("Unexported private RMI Registry at {}", this.registryPort);
            } catch (NoSuchObjectException nsoe) {
                // not expected, but don't really care either
                logger.info("Cannot unexport private RMI Registry reference", nsoe);
            }
        }
        this.registry = null;
    }

    @Override
    protected Object bindRepository(String name, Repository repository) {
        return new RmiRegistration(name, repository);
    }

    @Override
    protected void unbindRepository(String name, Object data) {
        RmiRegistration rr = (RmiRegistration) data;
        rr.unregister();
    }

    // ---------- support for private rmi registries ---------------------------

    /**
     * Tries to create a private registry at the configured port. If this fails
     * (for example because a registry already exists in the VM, a registry stub
     * for the port is returned. This latter stub may or may not connect to a
     * real registry, which may only be found out, when trying to register
     * repositories.
     */
    private Registry getPrivateRegistry() {
        if (this.registry == null) {
            try {
                // no, so try to create first
                this.registry = LocateRegistry.createRegistry(this.registryPort);
                this.registryIsPrivate = true;
                logger.info("Using private RMI Registry at {}", this.registryPort);

            } catch (RemoteException re) {
                // creating failed, check whether there is already one
                logger.info("Cannot create private registry, trying existing registry at {}, reason: {}", this.registryPort, re);

                try {
                    this.registry = LocateRegistry.getRegistry(this.registryPort);
                    this.registryIsPrivate = false;
                    logger.info("Trying existing registry at {}", this.registryPort);

                } catch (RemoteException pre) {
                    logger.error("Cannot get existing registry, will not register repositories on RMI", pre);
                }
            }
        }

        return this.registry;
    }

    /**
     * Returns a Jackrabbit JCR RMI <code>RemoteAdapterFactory</code> to be
     * used to publish local (server-side) JCR objects to a remote client.
     * <p>
     * This method returns an instance of the
     * <code>JackrabbitServerAdapterFactory</code> class to enable the use of
     * the Jackrabbit API over RMI. Extensions of this class may overwrite this
     * method to return a different implementation to provide different JCR
     * extension API depending on the server implementation.
     */
    protected RemoteAdapterFactory getRemoteAdapterFactory() {
        return new ServerAdapterFactory();
    }

    // ---------- Inner Class --------------------------------------------------

    private class RmiRegistration {

        private String rmiName;

        private Remote rmiRepository;

        RmiRegistration(String rmiName, Repository repository) {
            this.register(rmiName, repository);
        }

        public String getRmiName() {
            return this.rmiName;
        }

        public String getRmiURL() {
            String host;
            try {
                host = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (IOException ignore) {
                host = "localhost";
            }
            return "//" + host + ":" + RmiRegistrationSupport.this.registryPort
                + "/" + this.getRmiName();
        }

        private void register(String rmiName, Repository repository) {
            System.setProperty("java.rmi.server.useCodebaseOnly", "true");

            // try to create remote repository and keep it to ensure it is
            // unexported in the unregister() method
            try {
                RemoteAdapterFactory raf = getRemoteAdapterFactory();
                this.rmiRepository = raf.getRemoteRepository(repository);
            } catch (RemoteException e) {
                logger.error("Unable to create remote repository.", e);
                return;
            } catch (Exception e) {
                logger.error("Unable to create RMI repository. jcr-rmi.jar might be missing.", e);
                return;
            }

            try {
                // check whether we have a private registry already
                Registry registry = RmiRegistrationSupport.this.getPrivateRegistry();
                if (registry != null) {
                    registry.bind(rmiName, this.rmiRepository);
                    this.rmiName = rmiName;
                    logger.info("Repository bound to {}", this.getRmiURL());
                }

            } catch (NoSuchObjectException nsoe) {
                // the registry does not really exist
                logger.warn("Cannot contact RMI registry at {}, repository not registered", RmiRegistrationSupport.this.registryPort);
            } catch (Exception e) {
                logger.error("Unable to bind repository via RMI.", e);
            }
        }

        public void unregister() {
            // unregister repository
            if (this.rmiName != null) {
                try {
                    RmiRegistrationSupport.this.getPrivateRegistry().unbind(this.rmiName);
                    logger.info("Repository unbound from {}", this.getRmiURL());
                } catch (Exception e) {
                    logger.error("Error while unbinding repository from JNDI: ", e);
                }
            }

            // drop strong reference to remote repository
            if (this.rmiRepository != null) {
                try {
                    UnicastRemoteObject.unexportObject(this.rmiRepository, true);
                } catch (NoSuchObjectException nsoe) {
                    // not expected, but don't really care either
                    logger.info("Cannot unexport remote Repository reference", nsoe);
                }
            }
        }
    }

    @Override
    @Reference(service = LoggerFactory.class)
    protected void bindLogger(Logger logger) {
        super.bindLogger(logger);
    }

    @Override
    protected void unbindLogger(Logger logger) {
        super.unbindLogger(logger);
    }
}
