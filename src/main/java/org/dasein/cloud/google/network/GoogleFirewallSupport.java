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

package org.dasein.cloud.google.network;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.FirewallList;
import com.google.api.services.compute.model.Operation;
import org.apache.commons.lang.StringUtils;
import org.dasein.cloud.*;
import org.dasein.cloud.google.Google;
import org.dasein.cloud.google.common.NoContextException;
import org.dasein.cloud.google.util.ExceptionUtils;
import org.dasein.cloud.google.util.model.GoogleFirewalls;
import org.dasein.cloud.google.util.model.GoogleNetworks;
import org.dasein.cloud.google.util.model.GoogleOperations;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Implements the firewall services support in the Google API.
 *
 * @author Eduard Bakaev
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class GoogleFirewallSupport implements FirewallSupport {
	static private final Logger logger = Google.getLogger(GoogleFirewallSupport.class);

	private Google provider = null;

	GoogleFirewallSupport(Google provider) {
		this.provider = provider;
	}

	@Override
	public String[] mapServiceAction(ServiceAction action) {
		return new String[0];
	}

	@Override
	public String authorize(String firewallId, String source, Protocol protocol, int beginPort, int endPort)
			throws CloudException, InternalException {
		return authorize(firewallId, Direction.INGRESS, Permission.ALLOW, RuleTarget.getCIDR(source), protocol,
				RuleTarget.getGlobal(firewallId), beginPort, endPort, 0);
	}

	@Override
	public String authorize(String firewallId, Direction direction, String source, Protocol protocol, int beginPort, int endPort)
			throws CloudException, InternalException {
		if (direction.equals(Direction.INGRESS)) {
			return authorize(firewallId, direction, Permission.ALLOW, RuleTarget.getCIDR(source), protocol,
					RuleTarget.getGlobal(firewallId), beginPort, endPort, 0);
		} else {
			return authorize(firewallId, direction, Permission.ALLOW, RuleTarget.getGlobal(firewallId), protocol,
					RuleTarget.getCIDR(source), beginPort, endPort, 0);
		}
	}

	@Override
	public String authorize(String firewallId, Direction direction, Permission permission, String source, Protocol protocol,
	                        int beginPort, int endPort) throws CloudException, InternalException {
		if (direction.equals(Direction.INGRESS)) {
			return authorize(firewallId, direction, permission, RuleTarget.getCIDR(source), protocol,
					RuleTarget.getGlobal(firewallId), beginPort, endPort, 0);
		} else {
			return authorize(firewallId, direction, permission, RuleTarget.getGlobal(firewallId), protocol,
					RuleTarget.getCIDR(source), beginPort, endPort, 0);
		}
	}

	@Override
	public String authorize(String firewallId, Direction direction, Permission permission, String source, Protocol protocol,
	                        RuleTarget target, int beginPort, int endPort) throws CloudException, InternalException {
		if (direction.equals(Direction.INGRESS)) {
			return authorize(firewallId, direction, permission, RuleTarget.getCIDR(source), protocol, target, beginPort, endPort, 0);
		} else {
			return authorize(firewallId, direction, permission, target, protocol, RuleTarget.getCIDR(source), beginPort, endPort, 0);
		}
	}

	/**
	 * Appends firewall rules.
	 */
	@Override
	public String authorize(String firewallId, Direction direction, Permission permission, RuleTarget sourceEndpoint,
	                        Protocol protocol, RuleTarget destinationEndpoint, int beginPort,
	                        int endPort, int precedence) throws CloudException, InternalException {
		if (Permission.DENY.equals(permission)) {
			throw new OperationNotSupportedException("GCE does not support DENY rules");
		}
		if (direction.equals(Direction.EGRESS)) {
			throw new OperationNotSupportedException("GCE does not support EGRESS rules");
		}

		com.google.api.services.compute.model.Firewall googleFirewall = getGoogleFirewall(firewallId);
		Compute compute = provider.getGoogleCompute();
		Operation operation = null;
		try {
			List<com.google.api.services.compute.model.Firewall.Allowed> allowedList = googleFirewall.getAllowed();
			if (sourceEndpoint.getCidr() != null) {
				List<String> sourceRanges =
						googleFirewall.getSourceRanges() == null ? new ArrayList<String>() : googleFirewall.getSourceRanges();
				sourceRanges.add(sourceEndpoint.getCidr());
				googleFirewall.setSourceRanges(sourceRanges);
			}
			if (destinationEndpoint != null && sourceEndpoint.getProviderVirtualMachineId() != null) {
				List<String> sourceTags =
						googleFirewall.getSourceTags() == null ? new ArrayList<String>() : googleFirewall.getSourceTags();
				sourceTags.add(sourceEndpoint.getProviderVirtualMachineId());
				googleFirewall.setSourceTags(sourceTags);
			}
			if (destinationEndpoint != null && destinationEndpoint.getProviderVirtualMachineId() != null) {
				String target = destinationEndpoint.getProviderVirtualMachineId();
				List<String> targetTags = googleFirewall.getTargetTags();
				targetTags.add(target);
				googleFirewall.setTargetTags(targetTags);
			}

			com.google.api.services.compute.model.Firewall.Allowed allowed = GoogleFirewalls.getAllowed(protocol, beginPort, endPort);
			allowedList.add(allowed);
			googleFirewall.setAllowed(allowedList);

			Compute.Firewalls.Update update = compute.firewalls().update(provider.getContext().getAccountNumber(), firewallId, googleFirewall);
			operation = update.execute();
			operation.getStatus();
		} catch (IOException e) {
			logger.error("Failed to patch Firewall : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}

		FirewallRule rule = FirewallRule.getInstance(null, firewallId, sourceEndpoint, direction, protocol, permission,
				destinationEndpoint, beginPort, endPort);
		return rule.getProviderRuleId();
	}

	/**
	 * Appends target tag
	 *
	 * @param firewallId id of firewall the tags need to be set to
	 * @throws CloudException
	 * @throws InternalException
	 */
	public void appendTag(String firewallId, String targetTag) throws CloudException, InternalException {
		Preconditions.checkNotNull(targetTag);
		Preconditions.checkNotNull(firewallId);
		com.google.api.services.compute.model.Firewall googleFirewall = getGoogleFirewall(firewallId);
		Compute compute = provider.getGoogleCompute();
		try {
			List<String> targetTags = googleFirewall.getTargetTags() != null ? googleFirewall.getTargetTags() : new ArrayList<String>();
			targetTags.add(targetTag);
			googleFirewall.setTargetTags(targetTags);
			Compute.Firewalls.Update update = compute.firewalls().update(provider.getContext().getAccountNumber(), firewallId, googleFirewall);
			update.execute();
		} catch (IOException e) {
			logger.error("Failed to patch Firewall : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}
	}

	/**
	 * Creates Google Firewall in case provider isn't set. By default the provider set to 'default'
	 *
	 * @param name        of firewall to be created
	 * @param description description
	 * @return operation status value
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public String create(String name, String description) throws InternalException, CloudException {
		return createInVLAN(name, description, GoogleNetworks.DEFAULT);
	}

	/**
	 * Creates Google Firewall based on options.
	 *
	 * @param options for firewall
	 * @return operation status
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public String create(FirewallCreateOptions options) throws InternalException, CloudException {
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		Compute compute = provider.getGoogleCompute();
		Operation operation = null;
		try {
			com.google.api.services.compute.model.Firewall firewall =
					GoogleFirewalls.fromOptions(options, provider.getContext().getAccountNumber());
			Compute.Firewalls.Insert insertFirewall =
					compute.firewalls().insert(provider.getContext().getAccountNumber(), firewall);
			operation = insertFirewall.execute();
		} catch (IOException e) {
			logger.error("Failed to create the new firewall : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}

		GoogleOperations.logOperationStatusOrFail(operation);

		return StringUtils.substringAfterLast(operation.getTargetLink(), "/");
	}

	/**
	 * Creates Google Firewall in case provider is set.
	 *
	 * @param name        of firewall to be created
	 * @param description description
	 * @return operation status value
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public String createInVLAN(String name, String description, String providerVlanId) throws InternalException, CloudException {
		FirewallCreateOptions options = FirewallCreateOptions.getInstance(providerVlanId, name, description);
		return create(options);
	}

	/**
	 * Deletes Google firewall by its Id.
	 *
	 * @param firewallId to delete
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public void delete(String firewallId) throws InternalException, CloudException {
		Compute compute = provider.getGoogleCompute();
		Operation operation = null;
		try {
			Compute.Firewalls.Delete deleteAction = compute.firewalls().delete(provider.getContext().getAccountNumber(), firewallId);
			operation = deleteAction.execute();
		} catch (IOException e) {
			logger.error("Failed to delete Google Firewall object '" + firewallId + "' : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}

		GoogleOperations.logOperationStatusOrFail(operation);
	}

	/**
	 * Gets firewall for provided region.
	 *
	 * @return list available firewalls
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public Collection<Firewall> list() throws InternalException, CloudException {
		ProviderContext ctx = provider.getContext();
		if (ctx == null) {
			throw new CloudException("No context has been established for this request");
		}

		Compute compute = provider.getGoogleCompute();
		List<Firewall> cloudFirewallList = new ArrayList<Firewall>();
		try {
			Compute.Firewalls.List firewallsList = compute.firewalls().list(provider.getContext().getAccountNumber());
			FirewallList list = firewallsList.execute();
			if (list != null && list.size() > 0) {
				for (com.google.api.services.compute.model.Firewall firewall : list.getItems()) {
					Firewall cloudFirewall = GoogleFirewalls.toDaseinFirewall(firewall, ctx);
					if (cloudFirewall != null) {
						cloudFirewallList.add(cloudFirewall);
					}
				}
			}
		} catch (IOException e) {
			logger.error("Failed to get list of Firewalls : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}

		return cloudFirewallList;
	}

	/**
	 * Returns {@link Firewall} by its id.
	 *
	 * @param firewallId to search
	 * @return Cloud Firewall object
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public Firewall getFirewall(String firewallId) throws InternalException, CloudException {
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		com.google.api.services.compute.model.Firewall firewall = getGoogleFirewall(firewallId);
		if (firewall != null) {
			Firewall daseinFirewall = GoogleFirewalls.toDaseinFirewall(firewall, provider.getContext());
			Collection<FirewallRule> firewallRules = getRules(daseinFirewall.getName());
			daseinFirewall.setRules(firewallRules);
			return daseinFirewall;
		}

		return null;
	}

	/**
	 * Returns {@link com.google.api.services.compute.model.Firewall} firewall by its id.
	 *
	 * @param firewallId to search
	 * @return Cloud Firewall object
	 * @throws InternalException
	 * @throws CloudException
	 */
	private com.google.api.services.compute.model.Firewall getGoogleFirewall(String firewallId) throws InternalException, CloudException {
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		Compute compute = provider.getGoogleCompute();
		try {
			Compute.Firewalls.Get firewall = compute.firewalls().get(provider.getContext().getAccountNumber(), firewallId);
			return firewall.execute();
		} catch (IOException e) {
			logger.error("Failed to get list of Firewalls : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}

		return null;
	}

	@Override
	public String getProviderTermForFirewall(Locale locale) {
		return GoogleFirewalls.PROVIDER_TERM;
	}

	/**
	 * Gets firewall rules for specified firewall.
	 *
	 * @param firewallId of rules
	 * @return list of rules of current firewall
	 * @throws InternalException
	 * @throws CloudException
	 */
	@Override
	public Collection<FirewallRule> getRules(String firewallId) throws InternalException, CloudException {
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		Compute compute = provider.getGoogleCompute();
		try {
			Compute.Firewalls.Get firewall = compute.firewalls().get(provider.getContext().getAccountNumber(), firewallId);
			com.google.api.services.compute.model.Firewall firewallObj = firewall.execute();
			return GoogleFirewalls.toDaseinFirewallRules(firewallObj);
		} catch (IOException e) {
			logger.error("Failed to get list of Firewalls : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}

		return null;
	}

	@Override
	public Requirement identifyPrecedenceRequirement(boolean inVlan) throws InternalException, CloudException {
		return Requirement.NONE;
	}

	@Override
	public boolean isSubscribed() throws CloudException, InternalException {
		return true;
	}

	@Override
	public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
		throw new OperationNotSupportedException("Firewall rule precedence is not supported.");
	}

	@Override
	public Iterable<ResourceStatus> listFirewallStatus() throws InternalException, CloudException {
		Collection<ResourceStatus> firewallStatus = new ArrayList<ResourceStatus>();
		firewallStatus.add(new ResourceStatus(provider.getContext().getRegionId(), true));

		return firewallStatus;
	}

	@Override
	public Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
		Collection<RuleTargetType> destTypes = new ArrayList<RuleTargetType>();
		if (!inVlan) {
			destTypes.add(RuleTargetType.VLAN);
			destTypes.add(RuleTargetType.CIDR);
			destTypes.add(RuleTargetType.GLOBAL);
		}

		return destTypes;
	}

	@Override
	public Iterable<Direction> listSupportedDirections(boolean inVlan) throws InternalException, CloudException {
		Collection<Direction> directions = new ArrayList<Direction>();
		if (!inVlan) {
			directions.add(Direction.INGRESS);
		}

		return directions;
	}

	@Override
	public Iterable<Permission> listSupportedPermissions(boolean inVlan) throws InternalException, CloudException {
		Collection<Permission> permissions = new ArrayList<Permission>();
		if (!inVlan) {
			permissions.add(Permission.ALLOW);
			//			permissions.add(Permission.DENY);
		}

		return permissions;
	}

	@Override
	public Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan) throws InternalException, CloudException {
		Collection<RuleTargetType> sourceTypes = new ArrayList<RuleTargetType>();
		if (!inVlan) {
			sourceTypes.add(RuleTargetType.CIDR);
			sourceTypes.add(RuleTargetType.VLAN);
		}

		return sourceTypes;
	}

	@Override
	public void removeTags(String firewallId, Tag... tags) throws CloudException, InternalException {
		throw new OperationNotSupportedException("Google does not support firewall tags");
	}

	@Override
	public void removeTags(String[] firewallIds, Tag... tags) throws CloudException, InternalException {
		throw new OperationNotSupportedException("Google does not support firewall tags");
	}

	@Override
	public void revoke(String providerFirewallRuleId) throws InternalException, CloudException {
		FirewallRule firewallRule = FirewallRule.parseId(providerFirewallRuleId);
		String source = providerFirewallRuleId.split(":")[3];

		revoke(firewallRule.getFirewallId(), firewallRule.getDirection(), source, firewallRule.getProtocol(),
				firewallRule.getStartPort(), firewallRule.getEndPort());
	}

	@Override
	public void revoke(String firewallId, String source, Protocol protocol, int beginPort, int endPort)
			throws CloudException, InternalException {

		revoke(firewallId, Direction.INGRESS, source, protocol, beginPort, endPort);
	}

	@Override
	public void revoke(String firewallId, Direction direction, String source, Protocol protocol, int beginPort, int endPort)
			throws CloudException, InternalException {

		revoke(firewallId, direction, Permission.DENY, source, protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort);
	}

	@Override
	public void revoke(String firewallId, Direction direction, Permission permission, String source, Protocol protocol,
	                   int beginPort, int endPort) throws CloudException, InternalException {

		revoke(firewallId, direction, permission, source, protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort);
	}

	@Override
	public void revoke(String firewallId, Direction direction, Permission permission, String source, Protocol protocol,
	                   RuleTarget target, int beginPort, int endPort) throws CloudException, InternalException {
		com.google.api.services.compute.model.Firewall googleFirewall = getGoogleFirewall(firewallId);
		Compute compute = provider.getGoogleCompute();
		Operation operation = null;
		try {
			// revoke the cidr
			List<String> sourceRanges = googleFirewall.getSourceRanges();
			if (sourceRanges != null && sourceRanges.size() > 0) {
				List<String> sourceToSave = new ArrayList<String>();
				for (String sourceValue : sourceRanges) {
					if (!sourceValue.equals(source)) {
						sourceToSave.add(sourceValue);
					}
				}
				googleFirewall.setSourceRanges(sourceToSave);
			}

			//Setting default source range if it's null. Either source range or source tag has to be specified.
			if (googleFirewall.getSourceRanges() == null ||
					(googleFirewall.getSourceRanges() != null && googleFirewall.getSourceRanges().size() == 0)) {
				googleFirewall.setSourceRanges(new ArrayList<String>(Arrays.asList(GoogleFirewalls.DEFAULT_SOURCE_RANGE)));
			}

			List<String> sourceTags = googleFirewall.getSourceTags();
			if (sourceTags != null && sourceTags.size() > 0) {
				List<String> sourceTagsToSave = new ArrayList<String>();
				for (String sourceTag : sourceTags) {
					if (!sourceTag.equals(source)) {
						sourceTagsToSave.add(sourceTag);
					}
				}
				googleFirewall.setSourceTags(sourceTagsToSave);
			}

			List<String> targetTags = googleFirewall.getTargetTags();
			if (targetTags != null && targetTags.size() > 0) {
				List<String> targetTagsToSave = new ArrayList<String>();
				for (String targetTag : targetTags) {
					if (!targetTag.equals(target.getCidr())) {
						targetTagsToSave.add(targetTag);
					}
				}
				googleFirewall.setTargetTags(targetTagsToSave);
			}

			List<com.google.api.services.compute.model.Firewall.Allowed> allowedList = googleFirewall.getAllowed();
			if (allowedList != null && allowedList.size() > 0) {
				List<com.google.api.services.compute.model.Firewall.Allowed> allowedListToSave =
						new ArrayList<com.google.api.services.compute.model.Firewall.Allowed>();
				for (com.google.api.services.compute.model.Firewall.Allowed allowed : allowedList) {
					if (StringUtils.isNotEmpty(allowed.getIPProtocol())) {
						if (allowed.getIPProtocol().equals(protocol.name().toLowerCase())) {
							List<String> ports = allowed.getPorts();
							if (ports != null && ports.size() > 0) {
								String bPort = String.valueOf(beginPort);
								String toPort = endPort == -1 ? String.valueOf(beginPort) : String.valueOf(endPort);
								List<String> portsToSave = new ArrayList<String>();
								for (String portRange : allowed.getPorts()) {
									if (!(portRange.equals(bPort) || portRange.equals(bPort + "-" + toPort))) {
										portsToSave.add(portRange);
									}
								}
								//If there is no port set in 'allowed' object, this object has to be removed
								//as we cannot add a row when creating the new rule without setting ports.
								if (portsToSave.size() > 0) {
									allowed.setPorts(portsToSave);
									allowedListToSave.add(allowed);
								}
							} else {
								allowedListToSave.add(allowed);
							}
						} else {
							//if protocol isn't the same we save it.
							allowedListToSave.add(allowed);
						}
					}
				}

				//At least one allowed rule must be specified
				googleFirewall.setAllowed(allowedListToSave);
			}

			Compute.Firewalls.Update update = compute.firewalls().update(provider.getContext().getAccountNumber(), firewallId, googleFirewall);
			operation = update.execute();
			operation.getStatus();
		} catch (IOException e) {
			logger.error("Failed to patch Firewall : " + e.getMessage());
			ExceptionUtils.handleGoogleResponseError(e);
		}
	}

	@Override
	public boolean supportsRules(Direction direction, Permission permission, boolean inVlan)
			throws CloudException, InternalException {
		return (permission.equals(Permission.ALLOW) && direction.equals(Direction.INGRESS));
	}

	@Override
	public boolean supportsFirewallCreation(boolean inVlan) throws CloudException, InternalException {
		return true;
	}

	@Override
	public boolean supportsFirewallSources() throws CloudException, InternalException {
		return false;
	}

	@Override
	public void updateTags(String firewallId, Tag... tags) throws CloudException, InternalException {
		throw new OperationNotSupportedException("Google firewall does not contain meta data");
	}

	@Override
	public void updateTags(String[] firewallIds, Tag... tags) throws CloudException, InternalException {
		throw new OperationNotSupportedException("Google firewall does not contain meta data");
	}
}
