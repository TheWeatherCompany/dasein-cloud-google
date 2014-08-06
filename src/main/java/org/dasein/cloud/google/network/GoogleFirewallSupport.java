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
import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.dasein.cloud.*;
import org.dasein.cloud.google.Google;
import org.dasein.cloud.google.capabilities.GCEFirewallCapabilities;
import org.dasein.cloud.google.common.NoContextException;
import org.dasein.cloud.google.compute.server.OperationSupport;
import org.dasein.cloud.google.util.GoogleExceptionUtils;
import org.dasein.cloud.google.util.GoogleLogger;
import org.dasein.cloud.google.util.model.GoogleFirewalls;
import org.dasein.cloud.google.util.model.GoogleOperations;
import org.dasein.cloud.network.*;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Implements the firewall services support in the Google API.
 *
 * @author Eduard Bakaev
 * @version 2013.01 initial version
 * @since 2013.01
 */
public class GoogleFirewallSupport extends AbstractFirewallSupport {

	private static final Logger logger = GoogleLogger.getLogger(GoogleFirewallSupport.class);

	private Google provider = null;

	GoogleFirewallSupport(Google provider) {
		super(provider);
		this.provider = provider;
	}

	/**
	 * Adds target tag to single firewall
	 *
	 * @param targetTag  target tag
	 * @param firewallId firewall ID
	 * @throws CloudException in case of any errors
	 */
	public void addTargetLabel(String targetTag, String firewallId) throws InternalException, CloudException {
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
			GoogleExceptionUtils.handleGoogleResponseError(e);
		}
	}

	/**
	 * Removes google targetTag from single firewall
	 *
	 * @param targetTag  google target tag
	 * @param firewallId firewall ID
	 * @throws CloudException in case of any errors
	 */
	public void removeTargetLabel(String targetTag, String firewallId) throws InternalException, CloudException {
		Preconditions.checkNotNull(targetTag);
		Preconditions.checkNotNull(firewallId);

		com.google.api.services.compute.model.Firewall googleFirewall = getGoogleFirewall(firewallId);
		Compute compute = provider.getGoogleCompute();

		if (googleFirewall.getTargetTags() == null) {
			return;
		}

		try {
			Iterator<String> iterator = googleFirewall.getTargetTags().iterator();
			while (iterator.hasNext()) {
				if (targetTag.equals(iterator.next())) {
					iterator.remove();
				}
			}

			Compute.Firewalls.Update update = compute.firewalls().update(provider.getContext().getAccountNumber(), firewallId, googleFirewall);
			update.execute();
		} catch (IOException e) {
			logger.error("Failed to patch Firewall : " + e.getMessage());
			GoogleExceptionUtils.handleGoogleResponseError(e);
		}
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

        validateRules( options );

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
			GoogleExceptionUtils.handleGoogleResponseError(e);
		}

		OperationSupport operationSupport = provider.getComputeServices().getOperationsSupport();
		operationSupport.waitUntilOperationCompletes(operation, 180);

		return StringUtils.substringAfterLast(operation.getTargetLink(), "/");
	}

    private void validateRules(FirewallCreateOptions options) throws InternalException, CloudException {

        if ( options.getAuthorizeRules() == null || options.getAuthorizeRules().size() == 0 ) {
            return;
        }

        Iterable<Direction> supportedDirections = listSupportedDirections( options.getProviderVlanId() != null );
        Iterable<Permission> supportedPermissions = listSupportedPermissions( options.getProviderVlanId() != null );

        for ( FirewallRule rule : options.getAuthorizeRules() ) {
            if ( !Iterables.contains(supportedDirections, rule.getDirection() ) ) {
                throw new OperationNotSupportedException( "GCE does not support " + rule.getDirection() + " rules." );
            }
            if ( !Iterables.contains(supportedPermissions, rule.getPermission() ) ) {
                throw new OperationNotSupportedException( "GCE does not support " + rule.getPermission() + " rules." );
            }
        }
    }

	@Nullable
	@Override
	public Map<FirewallConstraints.Constraint, Object> getActiveConstraintsForFirewall(@Nonnull String firewallId) throws InternalException, CloudException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

  private transient volatile GCEFirewallCapabilities capabilities;
  @Override
  public @Nonnull GCEFirewallCapabilities getCapabilities() throws CloudException, InternalException{
    if(capabilities == null){
      capabilities = new GCEFirewallCapabilities(provider);
    }
    return capabilities;
  }

  @Nonnull
	@Override
	public FirewallConstraints getFirewallConstraintsForCloud() throws InternalException, CloudException {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public boolean supportsFirewallDeletion() throws CloudException, InternalException {
		throw new UnsupportedOperationException("Not implemented yet");
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
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		Compute compute = provider.getGoogleCompute();
		Operation operation = null;
		try {
			Compute.Firewalls.Delete deleteAction = compute.firewalls().delete(provider.getContext().getAccountNumber(), firewallId);
			operation = deleteAction.execute();
		} catch (IOException e) {
			logger.error("Failed to delete Google Firewall object '" + firewallId + "' : " + e.getMessage());
			GoogleExceptionUtils.handleGoogleResponseError(e);
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
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		Compute compute = provider.getGoogleCompute();
		List<Firewall> cloudFirewallList = new ArrayList<Firewall>();
		try {
			Compute.Firewalls.List firewallsList = compute.firewalls().list(provider.getContext().getAccountNumber());
			FirewallList list = firewallsList.execute();

			if (list == null || list.getItems() == null || list.getItems().isEmpty()) {
				return Collections.emptyList();
			}

			for (com.google.api.services.compute.model.Firewall firewall : list.getItems()) {
				Firewall cloudFirewall = GoogleFirewalls.toDaseinFirewall(firewall, ctx);
				if (cloudFirewall != null) {
					cloudFirewallList.add(cloudFirewall);
				}
			}
		} catch (IOException e) {
			logger.error("Failed to get list of Firewalls : " + e.getMessage());
			GoogleExceptionUtils.handleGoogleResponseError(e);
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
	public Firewall getFirewall(@Nonnull String firewallId) throws InternalException, CloudException {
		if (!provider.isInitialized()) {
			throw new NoContextException();
		}

		com.google.api.services.compute.model.Firewall firewall = getGoogleFirewall(firewallId);
		return GoogleFirewalls.toDaseinFirewall(firewall, provider.getContext());
	}

	/**
	 * Returns {@link com.google.api.services.compute.model.Firewall} firewall by its id.
	 *
	 * @param firewallId to search
	 * @return Cloud Firewall object
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
			GoogleExceptionUtils.handleGoogleResponseError(e);
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
			GoogleExceptionUtils.handleGoogleResponseError(e);
		}

		return Collections.emptyList();
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
        directions.add(Direction.INGRESS);

		return directions;
	}

	@Override
	public Iterable<Permission> listSupportedPermissions(boolean inVlan) throws InternalException, CloudException {
		Collection<Permission> permissions = new ArrayList<Permission>();
        permissions.add(Permission.ALLOW);

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
	public boolean supportsRules(Direction direction, Permission permission, boolean inVlan) throws CloudException, InternalException {
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

    @Override
    public void setTags(@Nonnull String firewallId, @Nonnull Tag... tags) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Google firewall does not contain meta data");
    }

    @Override
    public void setTags(@Nonnull String[] firewallIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Google firewall does not contain meta data");
    }
}
