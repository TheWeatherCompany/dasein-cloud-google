package org.dasein.cloud.google.common;

import com.google.api.services.compute.model.Operation;
import com.google.common.collect.Maps;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.google.api.services.compute.model.Operation.Error.Errors;

/**
 * Exceptions to be using when an operation fails on GCE side
 *
 * @author igoonich
 * @since 01.05.2014
 */
public class GoogleOperationFailedException extends CloudException {

	private static final long serialVersionUID = -4100288452798844941L;

	private static final Map<String, Integer> OPERATION_CODES = Maps.newHashMap();

	static {
		OPERATION_CODES.put("QUOTA_EXCEEDED", 400);
	}

	private GoogleOperationFailedException(@Nonnegative int httpCode, @Nullable String providerCode, @Nonnull String msg) {
		super(CloudErrorType.GENERAL, httpCode, providerCode, msg);
	}

	private GoogleOperationFailedException(@Nonnegative int httpCode, @Nullable String providerCode, @Nonnull String msg, @Nonnull Throwable cause) {
		super(CloudErrorType.GENERAL, httpCode, providerCode, msg, cause);
	}

	public static GoogleOperationFailedException create(Operation operation) {
		List<Errors> errors = operation.getError().getErrors();
		return create(operation, errors.get(0).getMessage());
	}

	public static GoogleOperationFailedException create(Operation operation, String errorMessage) {
		List<Errors> errors = operation.getError().getErrors();

		Errors firstError = errors.get(0);
		int httpStatusCode = OPERATION_CODES.containsKey(firstError.getCode())
				? OPERATION_CODES.get(firstError.getCode()) : operation.getHttpErrorStatusCode();

		return new GoogleOperationFailedException(httpStatusCode, operation.getHttpErrorMessage(), errorMessage);
	}

}
