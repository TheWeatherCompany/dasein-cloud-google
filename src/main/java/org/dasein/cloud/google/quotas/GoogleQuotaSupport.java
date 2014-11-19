package org.dasein.cloud.google.quotas;

import com.google.api.services.compute.model.Project;
import com.google.api.services.compute.model.Quota;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.google.AbstractOperation;
import org.dasein.cloud.google.Google;
import org.dasein.cloud.quotas.AbstractQuotaSupport;
import org.dasein.cloud.quotas.CloudResourceScope;
import org.dasein.cloud.quotas.CloudResourceType;
import org.dasein.cloud.quotas.QuotaDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * User: mgulimonov
 * Date: 18.11.2014
 */
public class GoogleQuotaSupport extends AbstractQuotaSupport {

    private final Google provider;

    private static final Map<String, CloudResourceType> RESOURCE_TYPE_MAP = new HashMap<String, CloudResourceType>();
    static {
        RESOURCE_TYPE_MAP.put("DISKS", CloudResourceType.VOLUME);
        RESOURCE_TYPE_MAP.put("FIREWALLS", CloudResourceType.FIREWALL);
        RESOURCE_TYPE_MAP.put("IMAGES", CloudResourceType.IMAGE);
        RESOURCE_TYPE_MAP.put("INSTANCES", CloudResourceType.INSTANCE);
        RESOURCE_TYPE_MAP.put("NETWORKS", CloudResourceType.NETWORK);
        RESOURCE_TYPE_MAP.put("ROUTES", CloudResourceType.ROUTE);
        RESOURCE_TYPE_MAP.put("SNAPSHOTS", CloudResourceType.SNAPSHOT);
        RESOURCE_TYPE_MAP.put("STATIC_ADDRESSES", CloudResourceType.IP_ADDRESS);
    }

    public GoogleQuotaSupport(Google provider) {
        this.provider = provider;
    }

    @Override
    public Collection<QuotaDescriptor> getQuotas(String regionId) throws CloudException, InternalException {
        Project project = provider.fetch(new AbstractOperation<Project>("GoogleQuotaSupport.getQuotas") {
            @Override
            public Project createOperation(Google google) throws IOException, CloudException {
                return provider.getGoogleCompute().projects().get(provider.getProject()).execute();
            }
        });

        return toQuotaDescriptors(project);
    }

    private Collection<QuotaDescriptor> toQuotaDescriptors(Project project) {
        Collection<QuotaDescriptor> result = new ArrayList<QuotaDescriptor>();
        for (Quota quota : project.getQuotas()) {
            CloudResourceType resourceType = resourceType(quota);
            if (resourceType != null) {
                result.add(new QuotaDescriptor(
                        resourceType,
                        CloudResourceScope.GLOBAL,
                        quota.getLimit().intValue(),
                        quota.getUsage().intValue()
                ));
            }
        }

        return result;
    }

    private CloudResourceType resourceType(Quota quota) {
        return RESOURCE_TYPE_MAP.get(quota.getMetric());
    }
}
