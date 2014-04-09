/**
 * Copyright (C) 2012-2013 Dell, Inc
 * See annotations for authorship information
 *
 * ====================================================================
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
 * ====================================================================
 */

package org.dasein.cloud.google.capabilities;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.google.Google;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.VLANCapabilities;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

public class GCENetworkCapabilities extends AbstractCapabilities<Google> implements VLANCapabilities{
  public GCENetworkCapabilities(@Nonnull Google cloud){super(cloud);}

  @Override public boolean allowsNewNetworkInterfaceCreation() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean allowsNewVlanCreation() throws CloudException, InternalException{
    return true;
  }

  @Override public boolean allowsNewRoutingTableCreation() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean allowsNewSubnetCreation() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean allowsMultipleTrafficTypesOverSubnet() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean allowsMultipleTrafficTypesOverVlan() throws CloudException, InternalException{
    return false;
  }

  @Override public int getMaxNetworkInterfaceCount() throws CloudException, InternalException{
    return -2;
  }

  @Override public int getMaxVlanCount() throws CloudException, InternalException{
    return -2;//GCE has a default limit of 5 but it can be changed
  }

  @Nonnull @Override public String getProviderTermForNetworkInterface(@Nonnull Locale locale){
    return "NIC";
  }

  @Nonnull @Override public String getProviderTermForSubnet(@Nonnull Locale locale){
    return "";
  }

  @Nonnull @Override public String getProviderTermForVlan(@Nonnull Locale locale){
    return "network";
  }

  @Nonnull @Override public Requirement getRoutingTableSupport() throws CloudException, InternalException{
    return Requirement.NONE;
  }

  @Nonnull @Override public Requirement getSubnetSupport() throws CloudException, InternalException{
    return Requirement.NONE;
  }

  @Nonnull @Override public Requirement identifySubnetDCRequirement() throws CloudException, InternalException{
    return Requirement.NONE;
  }

  @Override public boolean isNetworkInterfaceSupportEnabled() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean isSubnetDataCenterConstrained() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean isVlanDataCenterConstrained() throws CloudException, InternalException{
    return false;
  }

  @Nonnull @Override public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException{
    Collection<IPVersion> versions = new ArrayList<IPVersion>();
    versions.add(IPVersion.IPV4);
    return versions;
  }

  @Override public boolean supportsInternetGatewayCreation() throws CloudException, InternalException{
    return false;
  }

  @Override public boolean supportsRawAddressRouting() throws CloudException, InternalException{
    return false;
  }
}