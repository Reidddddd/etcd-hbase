package io.etcdhbase.core.lease;

import java.util.concurrent.CompletableFuture;

/** Resolves a Lease from the request's fixed auxiliary view before a KV operation attaches it. */
@FunctionalInterface
public interface LeaseStateLookup {
  CompletableFuture<LeaseState> requireAttachable(long leaseId);
}
