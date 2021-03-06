package org.dasein.cloud.google.util.model;

import com.google.api.client.util.DateTime;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.Disk;
import com.google.common.base.Function;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.google.common.InvalidResourceIdException;
import org.dasein.cloud.google.compute.server.GoogleServerSupport;
import org.dasein.cloud.google.util.GoogleEndpoint;
import org.dasein.cloud.google.util.GoogleLogger;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class contains a static factory methods that allows volumes to be converted to Dasein objects (and vice-a-versa). This class also
 * contains various methods for manipulating google volumes which are not provided by default via GCE java API.
 *
 * @author igoonich
 * @since 13.12.2013
 */
public final class GoogleDisks {

	private static final Logger logger = GoogleLogger.getLogger(GoogleDisks.class);

	/**
	 * Data center extension to be used by default
	 */
	private static final String DEFAULT_DISK_ZONE_TYPE = "a";

	/**
	 * Persistent disk type. Currently the only type of disks which accepted by google
	 */
	public static final String PERSISTENT_DISK_TYPE = "PERSISTENT";

	/**
	 * Scratch disk type. Is already deprecated by google, but still can be attached automatically by GCE
	 * when machine type ends with "-d" extension
	 */
	public static final String SCRATCH_DISK_TYPE = "SCRATCH";

	/**
	 * Google disks read mode
	 */
	public enum DiskMode {
		/**
		 * Read-only mode. Persistent disk can be attached to multiple instances in this mode.
		 */
		READ_ONLY,

		/**
		 * read-write mode. Persistent disk can be attached to a single instance in read-write mode.
		 */
		READ_WRITE
	}

	/**
	 * Google disks status
	 */
	public enum DiskStatus {
		CREATING, READY, RESTORING, FAILED, UNKNOWN;

		private static DiskStatus fromString(String status) {
			try {
				return valueOf(status);
			} catch (IllegalArgumentException e) {
				return UNKNOWN;
			}
		}

		public VolumeState asDaseinState() {
			switch (this) {
				case CREATING:
					return VolumeState.PENDING;
				case READY:
					return VolumeState.AVAILABLE;
				default:
					return VolumeState.DELETED;
			}
		}
	}

	/**
	 * Wrapper version of google attachment
	 */
	public static class RichAttachedDisk {
		private AttachedDisk attachedDisk;
		private AttachedDiskType attachedDiskType;

		public RichAttachedDisk(AttachedDiskType attachedDiskType, AttachedDisk attachedDisk) {
			this.attachedDiskType = checkNotNull(attachedDiskType);
			this.attachedDisk = checkNotNull(attachedDisk);
		}

		public AttachedDiskType getAttachedDiskType() {
			return attachedDiskType;
		}

		public AttachedDisk getAttachedDisk() {
			return attachedDisk;
		}
	}

	/**
	 * Dasein attached volume creation type
	 */
	public enum AttachedDiskType {

		/**
		 * Create boot volume operation
		 */
		BOOT,

		/**
		 * Do not create volume, use existing one
		 */
		EXISTING,

		/**
		 * Create standard volume
		 */
		STANDARD

	}

	/**
	 * Create {@link Disk} object based on provided dasein {@link VolumeCreateOptions}
	 *
	 * @param createOptions dasein volume create options
	 * @param context       provider context
	 * @return google disk object to be created
	 */
	public static Disk from(VolumeCreateOptions createOptions, ProviderContext context) {
		checkNotNull(createOptions);
		checkNotNull(context);
		checkNotNull(createOptions.getName(), "Name is missing for volume");

		Disk googleDisk = new Disk();

		googleDisk.setName(createOptions.getName());
		googleDisk.setDescription(createOptions.getDescription());

		if (createOptions.getSnapshotId() != null) {
			googleDisk.setSourceSnapshot(GoogleEndpoint.SNAPSHOT.getEndpointUrl(createOptions.getSnapshotId(), context.getAccountNumber()));
		}

		long volumeSizeGb = createOptions.getVolumeSize().getQuantity().longValue();
		if (volumeSizeGb != 0) {
			googleDisk.setSizeGb(volumeSizeGb);
		}

		if (createOptions.getDataCenterId() != null) {
			googleDisk.setZone(createOptions.getDataCenterId());
		} else {
			googleDisk.setZone(context.getRegionId() + "-" + DEFAULT_DISK_ZONE_TYPE);
		}

		return googleDisk;
	}

	/**
	 * Create {@link Disk} from image {@code sourceImageId} using provided create options {@link VolumeCreateOptions}
	 *
	 * @param createOptions dasein volume create options
	 * @return google disk object to be created
	 * @throws InvalidResourceIdException thrown if image ID is wrong
	 */
	public static Disk fromImage(String sourceImageId, VolumeCreateOptions createOptions) throws InvalidResourceIdException {
		checkNotNull(sourceImageId);
		checkNotNull(createOptions);

		Disk googleDisk = new Disk();
		googleDisk.setName(createOptions.getName());
		googleDisk.setDescription(createOptions.getDescription());

		googleDisk.setSourceImage(GoogleEndpoint.IMAGE.getEndpointUrl(sourceImageId));
		googleDisk.setZone(createOptions.getDataCenterId());
		googleDisk.setSizeGb(createOptions.getVolumeSize().getQuantity().longValue());

		return googleDisk;
	}

	public static AttachedDisk toAttachedDisk(Disk googleDisk) {
		checkNotNull(googleDisk);
		return new AttachedDisk()
				.setSource(googleDisk.getSelfLink())
				.setMode(DiskMode.READ_WRITE.toString())
				.setType(PERSISTENT_DISK_TYPE);
	}

	public static Volume toDaseinVolume(Disk googleDisk, ProviderContext context) {
		checkNotNull(googleDisk);
		checkNotNull(context);

		Volume volume = new Volume();

		// default properties
		volume.setType(VolumeType.HDD);
		volume.setProviderRegionId(context.getRegionId());

		volume.setProviderVolumeId(googleDisk.getName());
		volume.setName(googleDisk.getName());
		volume.setDescription(googleDisk.getDescription());
		volume.setSize(new Storage<Gigabyte>(googleDisk.getSizeGb(), Storage.GIGABYTE));

		volume.setProviderSnapshotId(googleDisk.getSourceSnapshot() != null
				? GoogleEndpoint.SNAPSHOT.getResourceFromUrl(googleDisk.getSourceSnapshot()) : null);

		volume.setProviderDataCenterId(googleDisk.getZone() != null
				? GoogleEndpoint.ZONE.getResourceFromUrl(googleDisk.getZone()) : null);
		volume.setCreationTimestamp(DateTime.parseRfc3339(googleDisk.getCreationTimestamp()).getValue());

		DiskStatus diskStatus = DiskStatus.fromString(googleDisk.getStatus());
		volume.setCurrentState(diskStatus.asDaseinState());

		return volume;
	}

	public static Volume toDaseinVolume(AttachedDisk attachedDisk) {
		Volume attachedVolume = new Volume();
		// only persistent disks have source
		if (PERSISTENT_DISK_TYPE.equals(attachedDisk.getType())) {
			attachedVolume.setName(GoogleEndpoint.VOLUME.getResourceFromUrl(attachedDisk.getSource()));
			attachedVolume.setProviderVolumeId(GoogleEndpoint.VOLUME.getResourceFromUrl(attachedDisk.getSource()));
		}
		// scratch disks are removed on instance termination
		if (SCRATCH_DISK_TYPE.equals(attachedDisk.getType())) {
			attachedVolume.setDeleteOnVirtualMachineTermination(true);
		}
		attachedVolume.setDeviceId(attachedDisk.getDeviceName());
		attachedVolume.setRootVolume(Boolean.TRUE.equals(attachedDisk.getBoot()));
		return attachedVolume;
	}

	/**
	 * Strategy for converting between google disks and dasein volumes
	 */
	public static final class ToDasinVolumeConverter implements Function<Disk, Volume> {
		private ProviderContext context;
		private GoogleServerSupport googleServerSupport;

		public ToDasinVolumeConverter(ProviderContext context) {
			this.context = context;
		}

		/**
		 * Configures and option to include a list of virtual machines connected to volume
		 *
		 * This method requires a google vms service in order to fetch all the instances connected to current volume
		 *
		 * @param googleServerSupport google instances support service
		 * @return same converter (builder variation)
		 */
		public ToDasinVolumeConverter withAttachedVirtualMachines(GoogleServerSupport googleServerSupport) {
			this.googleServerSupport = checkNotNull(googleServerSupport);
			return this;
		}

		@Nullable
		@Override
		public Volume apply(@Nullable Disk input) {
			Volume volume = GoogleDisks.toDaseinVolume(input, context);
			if (googleServerSupport != null) {
				includeVirtualMachines(volume);
			}
			return volume;
		}

		private void includeVirtualMachines(Volume volume) {
			try {
				Iterable<String> vmIds = googleServerSupport.getVirtualMachineNamesWithVolume(volume.getProviderVolumeId());
				// since only READ_WRITE disks are supported then it is expected the only one volume can be attached to instance
				if (vmIds.iterator().hasNext()) {
					volume.setProviderVirtualMachineId(vmIds.iterator().next());
				}
			} catch (Exception e) {
				logger.error("Failed to fetch virtual machines for volume '" + volume + "'", e);
			}
		}
	}

}
