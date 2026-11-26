#!/usr/bin/env python3
"""Validate registered M4 tests, MiniCluster traces, and source-bound run evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET


REPO = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO / "testkit/cases/m4-cases.json"
PROTOCOL = "etcd-v3.5.23"
KNOWN_TASKS = {"J40", "J41", "J42", "J43", "J44", "J45", "J46", "J47", "J29.4"}
GO_MODULE = "github.com/Reidddddd/etcd-hbase/testkit/go/"
JAVA_MODULES = {"protocol", "spi", "core", "hbase", "grpc", "admin", "server"}
JAVA_KINDS = {"surefire", "failsafe"}
RUN_SCHEMA = "etcd-hbase-m4-run/v1"
TRACE_SCHEMA = "etcd-hbase-trace/v1"
COMPARE_SUCCESS = "M4 Lease traces match the registered native comparison policy"
J41_COMPARE_SUCCESS = "M4 Lease binding traces match the registered native comparison policy"
J42_COMPARE_SUCCESS = "M4 KeepAlive traces match the registered native comparison policy"
J43_COMPARE_SUCCESS = "M4 Revoke traces match the registered native comparison policy"
REQUIRED_COMMANDS = {"go-module-lock", "go-tests", "maven-verify", "collect-maven-reports"}
J44_REQUIRED_COMMANDS = {"registry-check", "report-tests", "maven-verify", "collect-maven-reports"}
J45_REQUIRED_COMMANDS = {
    "registry-check", "report-tests", "go-tests", "native-watch-oracle",
    "maven-unit-tests", "collect-maven-reports",
}
J46_REQUIRED_COMMANDS = {
    "registry-check", "report-tests", "go-tests", "maven-verify", "collect-maven-reports",
}
J47_REQUIRED_COMMANDS = {
    "registry-check", "report-tests", "go-tests", "maven-verify", "collect-maven-reports",
}
J46_MAVEN_TEST_SELECTOR = (
    "-Dtest=WatchHistoryReplayTest,WatchStreamTest,WatchResponseFragmenterTest,WatchGatewayTest"
)
J46_MAVEN_IT_SELECTOR = "-Dit.test=M4WatchRpcIT"
J46_FAILSAFE_EMPTY_MODULES = "-Dfailsafe.failIfNoSpecifiedTests=false"
J47_MAVEN_TEST_SELECTOR = (
    "-Dtest=WatchHistoryReplayTest,WatchStreamTest,WatchGatewayTest,"
    "EventHistoryWorkerTest,M3TableGroupRuntimeWorkerLifecycleTest,"
    "SealedEventCursorTest,M3RuntimeConfigurationTest"
)
J47_MAVEN_IT_SELECTOR = (
    "-Dit.test=J47WatchReplayGcIT,M4WatchRpcIT#"
    "pinnedGoClientVerifiesHalfCloseAndDeadlineLifecycleOnBothLayouts+"
    "pinnedGoClientVerifiesRuntimeShutdownCancelsHalfClosedWatchOnBothLayouts+"
    "pinnedGoClientVerifiesRegistrationDiscoveryLifecycleOnBothLayouts+"
    "pinnedGoClientVerifiesCombinedLeaseAndWatchReplayLoadOnBothLayouts"
)
J47_FAILSAFE_EMPTY_MODULES = "-Dfailsafe.failIfNoSpecifiedTests=false"
J45_VERIFICATION_BOUNDARY = (
    "J45 public protocol evidence covers negative/zero startRevision cases, negative watch ID "
    "Create/Cancel, future startRevision Create/Cancel, and the Compact-boundary lifecycle. Future-event "
    "delivery, positive historical startRevision behavior, and replay/live handoff are assigned to J46.1. "
    "The native raw-proto event probe waits for etcd's synced progress marker as a harness barrier; this "
    "does not claim Java progress-request support, which is assigned to J46.2. Native raw-proto and local-service "
    "Go traces compare the registered start/cancel matrix; a separate Go-client lifecycle run exercises multiple "
    "watch IDs, byte ranges, filters/PrevKV, and client-context cancellation cleanup against "
    "Java WatchService, including two concurrently requested watchers sharing one pinned client stream. "
    "Normal server-side request half-close semantics remain assigned to J47. "
    "MiniCluster acceptance remains outside this unit evidence"
)

J44_JAVA_CLASS_MODULES = {
    "io.etcdhbase.core.commit.EventRevisionIndexSeekerTest": "core",
    "io.etcdhbase.core.commit.EventSubscriptionIndexTest": "core",
    "io.etcdhbase.core.commit.SealedEventCursorTest": "core",
    "io.etcdhbase.hbase.storage.HBaseEventRevisionIndexRepositoryTest": "hbase",
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT": "hbase",
    "io.etcdhbase.hbase.storage.J44EventIndexControlStoreIT": "hbase",
    "io.etcdhbase.hbase.storage.J44EventPinIT": "hbase",
    "io.etcdhbase.server.commit.EventBackfillCycleBudgetTest": "server",
    "io.etcdhbase.server.commit.EventBackfillExecutorTest": "server",
    "io.etcdhbase.server.commit.EventBackfillPublicationIT": "server",
    "io.etcdhbase.server.commit.EventHistoryWorkerTest": "server",
    "io.etcdhbase.server.commit.M3TableGroupRuntimeWorkerLifecycleTest": "server",
    "io.etcdhbase.server.commit.SerialMutationQueueTest": "server",
    "io.etcdhbase.server.observability.RequestMetricsTest": "server",
    "io.etcdhbase.server.M4LeaseRpcIT": "server",
}
J44_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.commit.EventRevisionIndexSeekerTest": {
        "mergesBucketsAcrossBoundedStepsWithoutTreatingPartialResultsAsComplete",
    },
    "io.etcdhbase.core.commit.EventSubscriptionIndexTest": {
        "matchesRawPointsAndHalfOpenRangesAndRemovesSubscriptions",
        "replacingRegistrationWithEmptyRangeRemovesPreviousPointOrInterval",
        "prunesDisjointIntervalsWithoutScanningEveryLowerBound",
        "returnsAllOverlappingIntervalsAcrossRotationsAndRemovals",
    },
    "io.etcdhbase.core.commit.SealedEventCursorTest": {
        "measuredProofReadsChargeActualEncodedBytesAndAllowNinthSmallRead",
        "measuredProofReadsStillRejectActualBytesBeyondTheirCumulativeLimit",
        "readsEventsInOrderWithPauseAndBoundedPageReads",
        "missingPageAndExhaustedBudgetFailWithoutAdvancingCursor",
        "sharedTailerReadsOnceAndIsolatesSlowSubscriptions",
        "watchRangeIndexMemoryIsAccountedAndReleasedOnCancellation",
        "sharedTailerDeliversOnlySparsePointAndRangeMatches",
        "sequenceReaderPersistsSelectedEventRootBeforeReadingItsIndex",
        "synchronousSequenceOpenFailureClosesTailerAndSubscriptions",
    },
    "io.etcdhbase.hbase.storage.J44EventIndexControlStoreIT": {
        "requiresDescriptorAndReopenedStoreForFencedControlUpgrade",
    },
    "io.etcdhbase.hbase.storage.J44EventPinIT": {
        "eventPinSurvivesReopenAndStopsPhysicalCollectionUntilReleased",
    },
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT": {
        "seeksEventRevisionsWithinOneBucketAndResumesExclusively",
    },
    "io.etcdhbase.server.commit.EventBackfillCycleBudgetTest": {
        "cycleFailsClosedWhenItsReservedAccountCannotBeAdmitted",
    },
    "io.etcdhbase.server.commit.EventBackfillExecutorTest": {
        "rechecksOwnerAndProtectionBeforePublishingTheSealedCheckpoint",
        "takeoverAfterSealingPreventsPublication",
        "cancellationBeforePreparationPreventsPublication",
    },
    "io.etcdhbase.server.commit.EventBackfillPublicationIT": {
        "resumesBackfillCycleAfterReopeningBothLayouts",
        "publishesAndReopensLegacyBackfillOnBothLayouts",
    },
    "io.etcdhbase.server.commit.EventHistoryWorkerTest": {
        "retriesResourceLimitAndOnlyCompletesAfterSuccessfulProgress",
        "stopCancelsActiveCycleAndWaitsForLaneCleanup",
        "rejectedSchedulingStopsAndReportsTheFailure",
        "slowBackfillReadDoesNotBlockHeartbeatAndPublicationUsesOwnerLane",
    },
    "io.etcdhbase.server.commit.M3TableGroupRuntimeWorkerLifecycleTest": {
        "eventHistoryWorkerStartsImmediatelyAndStartupReadinessWaitsForTailer",
        "ownerLossBeforeEventHistoryWorkerInstallationRejectsLateWorker",
        "ownerLossBeforeInstallationRejectsBothWorkers",
    },
    "io.etcdhbase.server.observability.RequestMetricsTest": {
        "recordsBoundedEventTailerCostAndLagWithoutKeyDimensions",
        "recordsBoundedEventHistoryFailures",
    },
    "io.etcdhbase.server.commit.SerialMutationQueueTest": {
        "backgroundWorkHasReservedCapacityAndRoundRobinsWithOtherLanes",
        "backgroundAdmissionCancellationAndCloseReleaseOnlyItsOwnBudget",
        "dispatchedCancellationDoesNotReleaseLaneUntilProducerDrains",
    },
    "io.etcdhbase.server.M4LeaseRpcIT": {
        "runtimeFencesEventIndexFormatAndPersistsSelectedPutAcrossLayouts",
        "realGoLeaseCallsMatchNativeAndSurviveRestartAcrossLayouts",
    },
}
J45_JAVA_CLASS_MODULES = {
    "io.etcdhbase.core.commit.SealedEventCursorTest": "core",
    "io.etcdhbase.grpc.service.SkeletonServiceTest": "grpc",
    "io.etcdhbase.server.commit.WatchGatewayTest": "server",
    "io.etcdhbase.server.commit.WatchStreamTest": "server",
    "io.etcdhbase.server.commit.WatchGoInteropTest": "server",
}
J45_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.commit.SealedEventCursorTest": {
        "sharedTailerAppliesInclusiveRevisionFloorsAndDropsEventsBeforeCreateBarrier",
        "subscriptionCapacityIsResourceLimitAndClosedTailerIsUnavailable",
    },
    "io.etcdhbase.grpc.service.SkeletonServiceTest": {
        "assembledWatchServiceDelegatesTheResponseObserverAndReturnsItsRequestHandler",
    },
    "io.etcdhbase.server.commit.WatchGatewayTest": {
        "mapsEveryTailerTerminationToItsTypedWatchTermination",
        "gatewayChargesRetainedRangeBytesAndReleasesThemOnClose",
        "tailerShutdownRacePreservesUnavailableAndReleasesWatchReservation",
        "passesNegativeStartRevisionAndCompactBoundaryAndReleasesBudgetsOnClose",
        "currentRevisionReadsTheAuthoritativeControlSnapshotInsteadOfTheTailerCheckpoint",
        "pollCancellationHandoffClosesTheDetachedDeliveryBeforeReleasingItsAccount",
        "workerRejectionAfterWatchSubscriptionOpensReleasesTailerAndBudgetResources",
        "workerErrorAfterWatchSubscriptionOpensReleasesTailerAndBudgetResources",
    },
    "io.etcdhbase.server.commit.WatchStreamTest": {
        "emitsCreatedBeforeInclusiveLiveEventsAndAppliesFilterAndPrevKv",
        "resourceLimitedCreateLeavesExistingWatchActive",
        "duplicateIdDoesNotReplaceExistingWatchAndCancelClosesOnlyItsSubscription",
        "emptyRequestAndUnknownCancelHaveNoResponseAndKeepActiveWatch",
        "publishedSubscriptionWakeupDrainsWithoutPollingTimerAndCloseClearsListener",
        "separateStreamsDrainPublishedEventsWithoutSchedulingPolls",
        "zeroStartWaitsForCreatedBarrierAndExcludesEarlierEvents",
        "initialNegativeStartWithNoHistoryUsesTheLiveBarrier",
        "negativeStartWithUncompactedHistorySkipsOldEventsAndDeliversNewEvents",
        "startBelowInitialCompactBoundaryCreatesThenCancelsAndKeepsIdUntilCancel",
        "negativeStartsBelowAnAdvancedCompactBoundaryReportTheNativeBoundary",
        "zeroStartUsesTheLiveBarrierEvenWhenTheCompactBoundaryIsPositive",
        "cancellationDuringPollClosesTheEventBeforeItCanEscapeTheWatcher",
        "cancelAcknowledgementWaitsForAndUsesTheCurrentSessionRevision",
        "createsAndCancelsRawWatchIdMinusOne",
        "createRejectionUsesTheCurrentSessionRevision",
        "terminalFailureWaitsForTheCurrentResponseWriter",
        "cancellingOneWatchKeepsOtherByteRangeSubscriptionActive",
        "resourceLimitTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries",
        "closedTailerTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries",
        "asynchronousTerminationRevisionFailureFailsStreamWithoutCanceledResponse",
        "lateTerminationRevisionDoesNotWriteAfterStreamClose",
        "processWideTombstonesFromOneStreamDoNotRejectAnotherStreamsActiveCreate",
        "oversizedCompleteRevisionCancelsWatchWithoutSendingPartialEvents",
        "watcherCloseStillClosesSubscriptionWhenPendingDeliveryCleanupThrowsError",
        "outputReleaseAttemptsEveryDeliveryAndPrioritizesFatalCleanupFailure",
    },
    "io.etcdhbase.server.commit.WatchGoInteropTest": {
        "pinnedGoClientMatchesStartAndLifecycleCasesAgainstJavaWatchService",
    },
}
J45_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeWatchStartWritesFreshTraceAndVerifiableMetadata",
        "TestParseRunOptionsPinsLocalWatchPhaseAndCheckpoint",
        "TestOpenEvidenceWritesLocalWatchCheckpointMetadata",
    },
    "internal/scenario": {
        "TestM4WatchStartScenarioMatchesPinnedNativeOracle",
        "TestM4WatchLifecycleMatchesPinnedNativeOracle",
        "TestLoadM4WatchStartSuiteRejectsMissingOrChangedCoverage",
        "TestValidateM4LeaseAcceptsLocalServiceEvidenceScope",
    },
    "internal/trace": {"TestTraceAcceptsLocalServiceEvidenceScope"},
}
J46_VERIFICATION_BOUNDARY = (
    "J46.1 validates inclusive positive historical replay, per-revision ordinal ordering, compaction "
    "registration races, and deterministic replay-to-live handoff at the captured (sequence, ordinal) "
    "barrier. J46.2 validates explicit and periodic progress barriers, event-before-progress ordering, "
    "filtered revisions, incomplete history, future starts, and the pinned etcd v3.5.23 raw-proto oracle. "
    "J46.3 validates bounded revision fragmentation, ordering and cancellation cleanup, plus the pinned Go "
    "client's fragment reassembly, automatic reconnection after first- and middle-fragment interruptions, "
    "and resumption at the next revision after a disconnect following a complete fragmented revision. "
    "Both recovery boundaries use the same Watch channel and verify replay-to-live handoff against Java "
    "WatchService on HBase MiniCluster with HISTORY and NATIVE layouts."
)
J46_JAVA_CLASS_MODULES = {
    "io.etcdhbase.server.commit.WatchHistoryReplayTest": "server",
    "io.etcdhbase.server.commit.WatchStreamTest": "server",
    "io.etcdhbase.server.commit.WatchResponseFragmenterTest": "server",
    "io.etcdhbase.server.commit.WatchGatewayTest": "server",
    "io.etcdhbase.server.M4WatchRpcIT": "server",
}
J46_JAVA_CLASS_KINDS = {
    "io.etcdhbase.server.commit.WatchHistoryReplayTest": "surefire",
    "io.etcdhbase.server.commit.WatchStreamTest": "surefire",
    "io.etcdhbase.server.commit.WatchResponseFragmenterTest": "surefire",
    "io.etcdhbase.server.commit.WatchGatewayTest": "surefire",
    "io.etcdhbase.server.M4WatchRpcIT": "failsafe",
}
J47_VERIFICATION_BOUNDARY = (
    "J47 validates startup readiness after event-index backfill and tailer installation, revokes readiness "
    "when the history worker fails or the tailer closes, and rejects event-history protection from another "
    "registry before storage reads. Production Lease activation requires the J51 identity, recovery and "
    "health capabilities; only trusted test wiring may select the public test profile. J47 then validates "
    "per-watch budget isolation when a transport is not ready, accepted asynchronous "
    "Watch creates across client half-close, Created-to-Canceled compact_revision delivery during "
    "historical replay, and cleanup after request-stream errors, client cancellation, deadline, late "
    "callbacks, physical producer drain and durable history-pin release. A real MiniCluster collector test "
    "opens WatchHistoryReplay against a durable event-pin registry and proves that the required EVENT row "
    "remains until replay settles, then becomes collectible after backfill and safe-watermark advancement. "
    "A pinned Go client also keeps an active Watch open after request-side half-close, then verifies that "
    "RuntimeServer shutdown terminates the RPC and that event-tailer HBase read counters remain stable "
    "after runtime, background identity provider, and request cleanup drain on both HBase MiniCluster "
    "layouts. This covers the existing shutdown lifecycle only; credential rotation/generation "
    "invalidation and cross-replica propagation remain assigned to J51. A pinned Go etcd client exercises "
    "the Milvus sessionutil registration shape against Java WatchService on both HBase MiniCluster layouts: "
    "one atomic multi-role registration binds expiring service keys, leaves the coordinator version key "
    "unleased, lists services at revision R, watches from R+1, renews at TTL/3, applies PrevKV deletes to "
    "local service lists, and preserves a role rebound to another Lease. A combined load case interleaves "
    "periodic KeepAlive between business writes while a Watch client is not reading, then replays the "
    "committed history through "
    "a bounded watcher burst. This is an isolated consumer fixture, "
    "not complete Milvus deployment acceptance. This evidence does not claim target credential rotation, "
    "production fanout capacity, or target-cluster acceptance."
)
J47_JAVA_CLASS_MODULES = {
    "io.etcdhbase.core.commit.SealedEventCursorTest": "core",
    "io.etcdhbase.server.commit.WatchHistoryReplayTest": "server",
    "io.etcdhbase.server.commit.WatchStreamTest": "server",
    "io.etcdhbase.server.commit.WatchGatewayTest": "server",
    "io.etcdhbase.server.commit.EventHistoryWorkerTest": "server",
    "io.etcdhbase.server.commit.M3TableGroupRuntimeWorkerLifecycleTest": "server",
    "io.etcdhbase.server.commit.J47WatchReplayGcIT": "server",
    "io.etcdhbase.server.M4WatchRpcIT": "server",
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest": "server",
}
J47_JAVA_CLASS_KINDS = {
    "io.etcdhbase.core.commit.SealedEventCursorTest": "surefire",
    "io.etcdhbase.server.commit.WatchHistoryReplayTest": "surefire",
    "io.etcdhbase.server.commit.WatchStreamTest": "surefire",
    "io.etcdhbase.server.commit.WatchGatewayTest": "surefire",
    "io.etcdhbase.server.commit.EventHistoryWorkerTest": "surefire",
    "io.etcdhbase.server.commit.M3TableGroupRuntimeWorkerLifecycleTest": "surefire",
    "io.etcdhbase.server.commit.J47WatchReplayGcIT": "failsafe",
    "io.etcdhbase.server.M4WatchRpcIT": "failsafe",
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest": "surefire",
}
J47_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.commit.SealedEventCursorTest": {
        "sequenceReaderRejectsProtectionFromAnotherEventRegistry",
    },
    "io.etcdhbase.server.commit.WatchHistoryReplayTest": {
        "compactAdvancingAfterReplayRegistrationPreservesTheBoundary",
        "compactDuringInitialIndexLookupSendsCreatedThenCanceledWithTheBoundary",
        "closingReplayWaitsForPhysicalProducerBeforeReleasingItsHistoryPin",
        "cancellingDuringProtectedEventOpenDrainsProducerBeforeReleasingReplayProtection",
        "authenticationAndPermissionFailuresRemainTypedDuringReplay",
        "authenticationAndPermissionFailuresDuringCompactRecheckRemainTyped",
    },
    "io.etcdhbase.server.commit.WatchStreamTest": {
        "compactionDuringHistoricalReplaySendsCompactRevisionAfterCreated",
        "slowWatchResourceLimitDoesNotBlockReadyWatcherAfterBackpressure",
        "clientHalfCloseKeepsActiveWatchOpenUntilTheRpcContextIsCancelled",
        "clientHalfCloseAllowsAnAcceptedCreateToFinishAsynchronously",
        "clientHalfCloseAllowsAnAcceptedSubscriptionToFinishAsynchronously",
        "emptyClientHalfCloseLeavesWatchRpcOpenLikeEtcd",
        "halfClosedRpcRemainsOpenAfterItsLastWatchIsCompacted",
        "requestStreamErrorDuringPollClosesTheDetachedDelivery",
        "replayAuthenticationAndPermissionFailuresUseTheirRpcStatus",
    },
    "io.etcdhbase.server.commit.WatchGatewayTest": {
        "pollCancellationHandoffClosesTheDetachedDeliveryBeforeReleasingItsAccount",
    },
    "io.etcdhbase.server.commit.EventHistoryWorkerTest": {
        "readinessRequiresSuccessfulCycleAndIsRevokedAfterWorkerFailure",
    },
    "io.etcdhbase.server.commit.M3TableGroupRuntimeWorkerLifecycleTest": {
        "eventHistoryWorkerStartsImmediatelyAndStartupReadinessWaitsForTailer",
        "readinessRequiresEventPinsAHealthyWorkerAndAnOpenTailer",
    },
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest": {
        "productionLoaderKeepsTheTrustedProfileAndCannotBeOverriddenByJson",
        "publicTestProfileDoesNotChangeOrdinaryM3RuntimeConfiguration",
    },
    "io.etcdhbase.server.commit.J47WatchReplayGcIT": {
        "watchReplayPinPreventsPhysicalEventCollectionUntilReplaySettles",
    },
    "io.etcdhbase.server.M4WatchRpcIT": {
        "pinnedGoClientVerifiesHalfCloseAndDeadlineLifecycleOnBothLayouts",
        "pinnedGoClientVerifiesRuntimeShutdownCancelsHalfClosedWatchOnBothLayouts",
        "pinnedGoClientVerifiesRegistrationDiscoveryLifecycleOnBothLayouts",
        "pinnedGoClientVerifiesCombinedLeaseAndWatchReplayLoadOnBothLayouts",
    },
}
J47_GO_TESTS = {
    "internal/scenario": {
        "TestM4WatchHalfCloseMatchesPinnedNativeOracle",
        "TestM4CombinedWatchErrorDiagnosticsOmitRawStatusMessages",
        "TestM4CombinedWatchResourceLimitErrorRecognizesRequestAndSubscriptionReasons",
    },
}
J46_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.server.commit.WatchHistoryReplayTest": {
        "replaysInclusiveStartInRevisionAndOrdinalOrder",
        "replayToLiveHandoffDropsTheWholeBarrierSequenceWithoutGapsOrDuplicates",
        "compactAdvancingDuringReplayRegistrationReturnsPerWatchCancellationBoundary",
    },
    "io.etcdhbase.server.commit.WatchStreamTest": {
        "explicitProgressFollowsAllEligibleEventsAndUsesTheAuthoritativeRevision",
        "explicitProgressCanAdvanceAcrossARevisionFilteredByTheWatch",
        "progressRequestIsDroppedWhenAnyWatchHasNotFinishedHistoricalReplay",
        "progressRequestIsDroppedUntilFutureStartRevisionIsReached",
        "periodicProgressNotifySupportsRawNegativeIdAndDrainsEventsBeforeItsWatermark",
        "periodicProgressNotifySkipsFutureAndUnsyncedWatchersIndependently",
        "fragmentsKeepTheRevisionContiguousAheadOfProgressAndOtherWatches",
        "closingDuringAFragmentSequenceReleasesTheUnfinishedRevision",
        "matchingCancelStopsTheRemainingFragmentsAndReleasesTheirDeliveries",
    },
    "io.etcdhbase.server.commit.WatchResponseFragmenterTest": {
        "leavesACompleteRevisionAsOneUnfragmentedResponseWhenItFits",
        "splitsLargeRevisionInOrderAndMarksOnlyNonFinalFragments",
        "packsMultipleEventsIntoEachFragmentWithoutChangingTheirOrder",
        "allowsTheFinalFragmentToUseItsSmallerFalseFlagEncoding",
        "includesPrevKvAndProtoOverheadWhenRejectingAnOversizedEvent",
        "passesControlResponsesWithoutFragmentingThem",
        "rejectsControlResponseThatExceedsTheMessageBudget",
        "rejectsEventsFromDifferentRevisions",
        "rejectsAlreadyFragmentedInputAndNonPositiveBudget",
    },
    "io.etcdhbase.server.commit.WatchGatewayTest": {
        "initiallyCompactedPositiveStartSkipsHistoryRegistrationAndKeepsPerWatchBoundary",
        "pollCancellationHandoffClosesTheDetachedDeliveryBeforeReleasingItsAccount",
    },
    "io.etcdhbase.server.M4WatchRpcIT": {
        "pinnedGoClientVerifiesFragmentAutoResumeAndLiveHandoffOnBothLayouts",
    },
}
J46_GO_TESTS = {
    "internal/scenario": {"TestM4WatchProgressRequestMatchesPinnedNativeOracle"},
}
J45_LOCAL_SERVICE_PHASES = (("initial", 2, -1), ("compacted", 8, 4))
J45_START_MATRIX = (
    ("initial-minus-two", "initial", -2, True, -1),
    ("initial-minus-one", "initial", -1, False, -1),
    ("initial-zero", "initial", 0, False, -1),
    ("initial-future", "initial", 3, False, -1),
    ("compacted-minus-two", "compacted", -2, True, "fixture"),
    ("compacted-minus-one", "compacted", -1, True, "fixture"),
    ("compacted-zero", "compacted", 0, False, "fixture"),
)
J45_RAW_WATCH_ID_MATRIX = (
    ("initial-watch-id-minus-one", "initial", -1, 0),
    ("compacted-watch-id-minus-one", "compacted", -1, 0),
)
J45_TRACE_CASES = (
    ("watch-start-history-put", "etcdserverpb.KV/Put"),
    ("initial-minus-two-created", "etcdserverpb.Watch/Watch"),
    ("initial-minus-two-compacted", "etcdserverpb.Watch/Watch"),
    ("initial-minus-two-duplicate-id", "etcdserverpb.Watch/Watch"),
    ("initial-minus-two-cancel-id", "etcdserverpb.Watch/Watch"),
    ("initial-minus-two-reused-created", "etcdserverpb.Watch/Watch"),
    ("initial-minus-two-reused-cancelled", "etcdserverpb.Watch/Watch"),
    ("initial-minus-one-created", "etcdserverpb.Watch/Watch"),
    ("initial-minus-one-cancelled", "etcdserverpb.Watch/Watch"),
    ("initial-zero-created", "etcdserverpb.Watch/Watch"),
    ("initial-zero-cancelled", "etcdserverpb.Watch/Watch"),
    ("initial-watch-id-minus-one-created", "etcdserverpb.Watch/Watch"),
    ("initial-watch-id-minus-one-cancelled", "etcdserverpb.Watch/Watch"),
    ("initial-future-created", "etcdserverpb.Watch/Watch"),
    ("initial-future-cancelled", "etcdserverpb.Watch/Watch"),
    ("watch-negative-live-created", "etcdserverpb.Watch/Watch"),
    ("watch-negative-live-progress", "etcdserverpb.Watch/Watch"),
    ("watch-negative-live-put", "etcdserverpb.KV/Put"),
    ("watch-negative-live-event", "etcdserverpb.Watch/Watch"),
    ("watch-negative-live-cancelled", "etcdserverpb.Watch/Watch"),
    ("watch-compact-fixture-put", "etcdserverpb.KV/Put"),
    ("watch-compact-fixture-range", "etcdserverpb.KV/Range"),
    ("watch-compact-fixture-compact", "etcdserverpb.KV/Compact"),
    ("compacted-minus-two-created", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-two-compacted", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-two-duplicate-id", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-two-cancel-id", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-two-reused-created", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-two-reused-cancelled", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-one-created", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-one-compacted", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-one-duplicate-id", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-one-cancel-id", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-one-reused-created", "etcdserverpb.Watch/Watch"),
    ("compacted-minus-one-reused-cancelled", "etcdserverpb.Watch/Watch"),
    ("compacted-zero-created", "etcdserverpb.Watch/Watch"),
    ("compacted-zero-cancelled", "etcdserverpb.Watch/Watch"),
    ("compacted-watch-id-minus-one-created", "etcdserverpb.Watch/Watch"),
    ("compacted-watch-id-minus-one-cancelled", "etcdserverpb.Watch/Watch"),
)

J40_TRACE_CASES = (
    ("native-baseline", "etcdserverpb.Lease/LeaseLeases"),
    ("auto-grant", "etcdserverpb.Lease/LeaseGrant"),
    ("auto-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("explicit-grant", "etcdserverpb.Lease/LeaseGrant"),
    ("explicit-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("duplicate-id", "etcdserverpb.Lease/LeaseGrant"),
    ("missing-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("negative-id", "etcdserverpb.Lease/LeaseGrant"),
    ("leases-list", "etcdserverpb.Lease/LeaseLeases"),
    ("oversized-ttl", "etcdserverpb.Lease/LeaseGrant"),
    ("zero-id-ttl", "etcdserverpb.Lease/LeaseTimeToLive"),
)
J41_TRACE_CASES = (
    ("binding-baseline", "etcdserverpb.Lease/LeaseLeases"),
    ("binding-grant-a", "etcdserverpb.Lease/LeaseGrant"),
    ("binding-grant-b", "etcdserverpb.Lease/LeaseGrant"),
    ("binding-register-then", "etcdserverpb.KV/Txn"),
    ("binding-ttl-a-initial", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-initial", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ignore-flags", "etcdserverpb.KV/Put"),
    ("binding-ttl-a-preserved", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-rebind-a-to-b", "etcdserverpb.KV/Put"),
    ("binding-ttl-a-after-rebind", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-after-rebind", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-register-else", "etcdserverpb.KV/Txn"),
    ("binding-ttl-b-after-else", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-register-nested", "etcdserverpb.KV/Txn"),
    ("binding-ttl-a-after-nested", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-after-nested", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-unbind", "etcdserverpb.KV/Put"),
    ("binding-ttl-a-after-unbind", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-delete", "etcdserverpb.KV/DeleteRange"),
    ("binding-ttl-b-after-delete", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-recreate-unleased", "etcdserverpb.KV/Put"),
    ("binding-ttl-b-after-recreate", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-delete-range", "etcdserverpb.KV/DeleteRange"),
    ("binding-ttl-a-after-delete-range", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-ttl-b-after-delete-range", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-role-prefix-after-delete-range", "etcdserverpb.KV/Range"),
    ("binding-version-after-delete-range", "etcdserverpb.KV/Range"),
    ("binding-survivor-put", "etcdserverpb.KV/Put"),
    ("binding-ttl-b-after-survivor-put", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-invalid-lease-atomicity", "etcdserverpb.KV/Txn"),
    ("binding-atomicity-range-first", "etcdserverpb.KV/Range"),
    ("binding-atomicity-range-second", "etcdserverpb.KV/Range"),
    ("binding-ttl-a-after-failed-txn", "etcdserverpb.Lease/LeaseTimeToLive"),
    ("binding-invalid-lease-put", "etcdserverpb.KV/Put"),
    ("binding-atomicity-range-invalid-put", "etcdserverpb.KV/Range"),
    ("binding-invalid-lease-unselected-branch", "etcdserverpb.KV/Txn"),
    ("binding-unselected-then-absent", "etcdserverpb.KV/Range"),
    ("binding-unselected-else-visible", "etcdserverpb.KV/Range"),
    ("binding-ttl-b-after-unselected-branch", "etcdserverpb.Lease/LeaseTimeToLive"),
)
J42_TRACE_CASES = (
    ("native-baseline", "etcdserverpb.Lease/LeaseLeases", "L05"),
    ("keepalive-grant-a", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("keepalive-grant-b", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("keepalive-grant-managed-a", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("keepalive-grant-managed-b", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("keepalive-range-before", "etcdserverpb.KV/Range", "R01"),
    ("keepalive-stream-a", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-stream-b", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-stream-missing", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-stream-a-repeat", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-ttl-a", "etcdserverpb.Lease/LeaseTimeToLive", "L04"),
    ("keepalive-ttl-b", "etcdserverpb.Lease/LeaseTimeToLive", "L04"),
    ("keepalive-managed-a-first", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-managed-a-second", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-managed-b-first", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-managed-b-second", "etcdserverpb.Lease/LeaseKeepAlive", "L02"),
    ("keepalive-range-after", "etcdserverpb.KV/Range", "R01"),
)
M4_STREAM_METHODS = {
    "etcdserverpb.Lease/LeaseKeepAlive",
    "etcdserverpb.Watch/Watch",
}
J42_STREAM_TERMINALS = {"keepalive-stream-a-repeat": {"kind": "eof"}}
J43_TRACE_CASES = (
    ("native-baseline", "etcdserverpb.Lease/LeaseLeases", "L05"),
    ("revoke-grant-bound", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("revoke-put-bound-key-a", "etcdserverpb.KV/Put", "P01"),
    ("revoke-put-bound-key-b", "etcdserverpb.KV/Put", "P01"),
    ("revoke-range-before-a", "etcdserverpb.KV/Range", "R01"),
    ("revoke-range-before-b", "etcdserverpb.KV/Range", "R01"),
    ("revoke-bound", "etcdserverpb.Lease/LeaseRevoke", "L03"),
    ("revoke-range-after-a", "etcdserverpb.KV/Range", "R01"),
    ("revoke-range-after-b", "etcdserverpb.KV/Range", "R01"),
    ("revoke-ttl-after", "etcdserverpb.Lease/LeaseTimeToLive", "L04"),
    ("revoke-missing", "etcdserverpb.Lease/LeaseRevoke", "L03"),
    ("revoke-grant-empty", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("revoke-empty", "etcdserverpb.Lease/LeaseRevoke", "L03"),
    ("revoke-empty-ttl", "etcdserverpb.Lease/LeaseTimeToLive", "L04"),
    ("expiry-grant", "etcdserverpb.Lease/LeaseGrant", "L01"),
    ("expiry-put-bound-key-a", "etcdserverpb.KV/Put", "P01"),
    ("expiry-put-bound-key-b", "etcdserverpb.KV/Put", "P01"),
    ("expiry-range-before-a", "etcdserverpb.KV/Range", "R01"),
    ("expiry-range-before-b", "etcdserverpb.KV/Range", "R01"),
    ("expiry-ttl-after", "etcdserverpb.Lease/LeaseTimeToLive", "L04"),
    ("expiry-range-after-a", "etcdserverpb.KV/Range", "R01"),
    ("expiry-range-after-b", "etcdserverpb.KV/Range", "R01"),
)
J43_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeLeaseRevokeWritesFreshTraceAndVerifiableMetadata",
        "TestCompareRevokeRejectsCorruptOrEmptyTrace",
    },
    "internal/scenario": {
        "TestM4LeaseRevokeScenarioMatchesItsPinnedNativeOracle",
        "TestLoadM4LeaseRevokeSuiteRejectsMissingOrChangedCoverage",
    },
    "internal/compare": {
        "TestCompareM4LeaseRevokeRequiresOneRevisionForAllBoundKeys",
        "TestCompareM4LeaseRevokeRejectsChangedTerminalState",
    },
}
J43_JAVA_CLASS_MODULES = {
    "io.etcdhbase.core.lease.LeaseExpiryCheckpointTest": "core",
    "io.etcdhbase.core.lease.LeaseExpiryStepCommandTest": "core",
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT": "hbase",
    "io.etcdhbase.server.commit.BoundedSystemLeaseTimeSourceTest": "server",
    "io.etcdhbase.server.commit.LeaseExpiryStepExecutorTest": "server",
    "io.etcdhbase.server.commit.LeaseExpiryWorkerTest": "server",
    "io.etcdhbase.server.commit.LeaseRevocationExecutorTest": "server",
    "io.etcdhbase.server.commit.LeaseRenewalExecutorTest": "server",
    "io.etcdhbase.server.M4LeaseKeepAliveRpcIT": "server",
    "io.etcdhbase.server.M4LeaseRevokeRpcIT": "server",
    "io.etcdhbase.server.M4MixedLeaseBenchmarkIT": "server",
}
J43_JAVA_CLASSES = set(J43_JAVA_CLASS_MODULES)
J43_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT": {
        "seeksLatestExpiryCheckpointAtTheReadBarrier",
    },
    "io.etcdhbase.server.commit.BoundedSystemLeaseTimeSourceTest": {
        "wallClockStepPausesTrustedTimeAndReportsOnceUntilTheClockReturns",
        "backwardWallClockStepAlsoPausesUntilTheClockCatchesUp",
        "rejectsMonotonicRegressionAndWallTimeOverflow",
    },
    "io.etcdhbase.server.commit.LeaseExpiryStepExecutorTest": {
        "scanUsesItsPinnedSequenceToCoverCandidatesWrittenAfterThePriorCheckpoint",
        "candidateSequenceCannotBeZeroOrUseANegativeBarrier",
        "expiryPageBudgetRemainsReservedUntilThePhysicalScanDrains",
        "synchronousScanFailureReleasesPageBudgetAfterScopeDrain",
    },
    "io.etcdhbase.server.commit.LeaseExpiryWorkerTest": {
        "successfulCloseWaitsForBothCleanupAndPhysicalProducerCompletion",
        "staleExpiryWorkerDoesNotForwardAfterOwnerChangesDuringCheckpointRead",
        "asynchronousCheckpointReadFailureClosesPreviouslyEstablishedOwnerReadiness",
        "healthyStandbyWorkerSignalsInitialReadinessWithoutAnOwnerCheckpoint",
    },
    "io.etcdhbase.server.commit.LeaseRenewalExecutorTest": {
        "expiredActiveLeaseUsesTheAtomicRevocationResultForKeepAlive",
        "failedExpiryRevocationCannotTurnIntoASuccessfulKeepAliveResponse",
    },
    "io.etcdhbase.server.commit.LeaseRevocationExecutorTest": {
        "unhealthyClockAtSelectionRejectsAnEarlierExpiryObservation",
        "lowerBoundBeforeDeadlineRejectsAnEarlierExpiryObservation",
        "healthyLowerBoundAtDeadlineAllowsExpirySelection",
    },
    "io.etcdhbase.server.M4LeaseRevokeRpcIT": {
        "realGoLeaseRevokeMatchesNativeAcrossLayouts",
        "expiryCheckpointResumesAfterRuntimeRestart",
        "revokeAndKeepAliveRaceHasOneDurableTerminalOutcome",
        "staleExpiryCandidatesPreserveReboundAndUnleasedRecreatedKeys",
        "largeLeaseRevokeDeletesEveryBindingInOneRevision",
    },
    "io.etcdhbase.server.M4MixedLeaseBenchmarkIT": {
        "emitsMixedLeaseLoadEvidenceAtIncreasingConcurrency",
    },
}
J29_4_SOURCE_FILES = (
    "core/src/main/java/io/etcdhbase/core/lease/LeaseExpiryCheckpoint.java",
    "server/src/main/java/io/etcdhbase/server/commit/LeaseExpiryWorker.java",
    "server/src/main/java/io/etcdhbase/server/commit/LeaseRevocationExecutor.java",
    "server/src/main/java/io/etcdhbase/server/observability/RequestMetrics.java",
    "server/src/test/java/io/etcdhbase/server/M4LeaseRpcIT.java",
    "server/src/test/java/io/etcdhbase/server/M4MixedLeaseBenchmarkIT.java",
    "scripts/verify/m4-report.py",
    "testkit/cases/m4-cases.json",
)
J40_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeLeaseWritesFreshTraceAndVerifiableMetadata",
        "TestParseRunOptionsRejectsMissingLayoutAndUnscopedPrefix",
        "TestCompareRejectsCorruptOrEmptyTrace",
    },
    "internal/scenario": {
        "TestM4LeaseScenarioMatchesItsPinnedNativeOracle",
        "TestLoadM4LeaseSuiteRejectsMissingOrChangedCoverage",
        "TestValidateM4LeaseRejectsUnscopedPrefixBeforeRPC",
    },
    "internal/compare": {
        "TestCompareM4LeaseAllowsConfiguredMinimumAndAtMostOneSecondRemainingTTLSkew",
        "TestCompareM4LeaseRejectsRemainingTTLSkewOverOneSecond",
        "TestCompareM4LeaseKeepsStatusAndErrorContractsStrict",
    },
    "internal/trace": {
        "TestSnapshotProtoPreservesBinaryAndInt64Values",
        "TestReadJSONLRejectsInvalidOrderingAndSecrets",
        "TestStreamTerminalCannotContradictItsOutcome",
    },
}
J40_JAVA_CLASSES = {
    "io.etcdhbase.core.lease.LeaseStateTest",
    "io.etcdhbase.core.lease.LeaseGrantPlannerTest",
    "io.etcdhbase.core.lease.LeaseIdAllocatorTest",
    "io.etcdhbase.core.commit.AuxiliaryFormatUpgradeTest",
    "io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifierTest",
    "io.etcdhbase.core.commit.SystemRecordAuxiliaryMaterializationStoreTest",
    "io.etcdhbase.core.commit.LeaseReadViewTest",
    "io.etcdhbase.core.commit.CommitFinalizationServiceTest",
    "io.etcdhbase.core.commit.ManifestPreparationTest",
    "io.etcdhbase.grpc.service.LeaseServiceTest",
    "io.etcdhbase.grpc.protocol.LeaseGrantResponseMapperTest",
    "io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapperTest",
    "io.etcdhbase.grpc.RequestLifecycleOutcomeTest",
    "io.etcdhbase.server.commit.LeaseGrantPreparationTest",
    "io.etcdhbase.server.commit.BoundedSystemLeaseTimeSourceTest",
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest",
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT",
    "io.etcdhbase.server.M4LeaseRpcIT",
}
J40_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.commit.CommitPublisherTest": {
        "selectionConditionIsRecheckedAfterHandoffAndBeforeEachRetry",
        "selectedOrUnknownCasIsResolvedWithoutRepeatingSelectionCondition",
    },
    "io.etcdhbase.server.M3RuntimeBudgetsTest": {
        "m4MinimumAccountsForEveryGroupAndConcurrentLeaseOwner",
    },
    "io.etcdhbase.core.lease.LeaseStateTest": {"deadlineUsesUpperBoundAndExpiryUsesLowerBound"},
    "io.etcdhbase.core.commit.LeaseReadViewTest": {
        "bindingReadMergesStableHistoryWithSelectedChangesAtOneBarrier"
    },
    "io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapperTest": {
        "includesEveryRequestedKeyAsOriginalBytesWithThePinnedHeader"
    },
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT": {
        "scansOneBucketInBoundedExclusiveResumePages"
    },
    "io.etcdhbase.server.M4LeaseRpcIT": {
        "realGoLeaseCallsMatchNativeAndSurviveRestartAcrossLayouts",
        "minimumMemoryPreservesLeaseLifecycleAcrossClockFailures",
    },
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest": {
        "m4MemoryMinimumIncludesGatewayOwnerAndRecoveryReservations",
    },
    "io.etcdhbase.server.commit.BoundedSystemLeaseTimeSourceTest": {
        "returnsConservativeBoundsWhileWallAndMonotonicClocksAdvanceTogether",
        "wallClockStepPausesTrustedTimeAndReportsOnceUntilTheClockReturns",
        "backwardWallClockStepAlsoPausesUntilTheClockCatchesUp",
        "detectsCumulativeDriftAcrossManySmallWallClockChanges",
        "clockReadFailureRecoversOnlyAfterASuccessfulTimeObservation",
        "rejectsMonotonicRegressionAndWallTimeOverflow",
        "includesClockReadWindowInUpperBoundAtConfiguredLimit",
        "allowsOnlyDriftExplainedByMonotonicSamplingWindows",
        "rejectsClockReadWindowBeyondConfiguredLimit",
        "runtimeReadinessBecomesFalseOnDiscontinuityAndTrueAfterRecovery",
    },
}
J41_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeLeaseBindingWritesFreshTraceAndVerifiableMetadata",
        "TestCompareBindingsRejectsCorruptOrEmptyTrace",
    },
    "internal/scenario": {
        "TestM4LeaseBindingScenarioMatchesItsPinnedNativeOracle",
        "TestLoadM4LeaseBindingSuiteRejectsMissingOrChangedCoverage",
    },
    "internal/compare": {
        "TestCompareM4LeaseBindingRecordsNormalizesTransportAndScenarioRelativeTtl",
        "TestCompareM4LeaseBindingRecordsRejectsRevisionAndPartialResult",
        "TestCompareM4LeaseBindingRecordsRejectsInvalidTtlAndUnexpectedRenewal",
        "TestCompareM4LeaseBindingRequiresNotFoundForInvalidLeaseOperations",
        "TestCompareM4LeaseBindingRejectsTtlRecoveryAfterDecrease",
        "TestCompareM4LeaseBindingRejectsAccumulatedTtlTolerance",
        "TestCompareM4LeaseBindingAllowsBoundedRoundingAfterDecrease",
        "TestCompareM4LeaseBindingTracksTwoLeaseClocksIndependently",
    },
    "internal/trace": {
        "TestSnapshotProtoPreservesBinaryAndInt64Values",
        "TestReadJSONLRejectsInvalidOrderingAndSecrets",
        "TestStreamTerminalCannotContradictItsOutcome",
    },
}
J41_JAVA_CLASSES = {
    "io.etcdhbase.server.commit.LeaseRevocationPreparerTest",
    "io.etcdhbase.core.lease.LeaseBindingStatePlannerTest",
    "io.etcdhbase.core.commit.LeaseBindingFormatUpgradeTest",
    "io.etcdhbase.core.commit.CommitFinalizationServiceTest",
    "io.etcdhbase.core.kv.LeaseBindingChangeSetTest",
    "io.etcdhbase.core.lease.LeaseBindingIdentityTest",
    "io.etcdhbase.core.lease.LeaseCostAdmissionTest",
    "io.etcdhbase.core.commit.LeaseReadViewTest",
    "io.etcdhbase.core.commit.AuxiliaryLeasePlanVerifierTest",
    "io.etcdhbase.hbase.codec.LeaseStateMessagesTest",
    "io.etcdhbase.hbase.codec.SystemRecordKeyCodecTest",
    "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT",
    "io.etcdhbase.server.M4LeaseRpcIT",
    "io.etcdhbase.server.M4LeaseBindingRpcIT",
    "io.etcdhbase.server.commit.LeaseBindingMutationPlannerTest",
    "io.etcdhbase.server.commit.M3OwnerLeaseTest",
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest",
}
J41_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.server.commit.LeaseRevocationPreparerTest": {
        "preparesConsistentDeletesPrevKvUnbindsAndTerminalStateUntilClosed",
        "secondKvReadFailureRejectsTheWholeResultAndReleasesReservations",
        "mismatchedKvLeaseOrCreateRevisionRejectsTheWholeResult",
        "readerConstructionFailureReleasesTheAlreadyRetainedCandidateMemory",
        "insufficientCandidateCapacityFailsBeforeReadingKv",
        "finishFailureReleasesAllPreparedMemoryWithoutReturningAPartialPlan",
        "throwingFinishSupplierReleasesAllPreparedMemory",
        "cancellationKeepsPinAndBudgetUntilThePhysicalKvReadStops",
        "cancelledWaiterReclaimsALateSuccessfulPreparedResult",
        "cancellationDuringPinFinishCannotReturnASuccessfulPlan",
    },
    "io.etcdhbase.core.lease.LeaseBindingStatePlannerTest": {
        "attachingUpdatingAndRecreatingChargeOnlyNewBindingHistory",
        "rebindingAndUnbindingChargeEachLeaseItsOwnHistoryVersion",
        "revocationRemovesLiveCostsAndRetainsHistoryAndExpiryIdentity",
        "revocationCannotLeaveLiveBindingsOrReservedCost",
        "emptyRevocationRetainsAlreadyChargedHistory",
        "attachmentReservesFutureUnbindAtTheExactCapacityBoundary",
        "resultsAreOrderedImmutableAndDoNotRetainRequestState",
        "invalidBaseAndAccountingUnderflowFailWithoutAPlan",
        "bindingCountOverflowDoesNotWrapIntoAnAdmissiblePlan",
    },
    "io.etcdhbase.core.kv.LeaseBindingChangeSetTest": {
        "deleteAndUnleasedRecreationRetainTheOriginalUnbind"
    },
    "io.etcdhbase.core.lease.LeaseBindingIdentityTest": {
        "isolatesBindingRowsAcrossIncarnationsOfTheSameLeaseId"
    },
    "io.etcdhbase.core.commit.CommitFinalizationServiceTest": {
        "selectedLeaseBindingRecordIsReplayableAfterRestartAtItsMaterializationBoundary"
    },
    "io.etcdhbase.core.lease.LeaseCostAdmissionTest": {
        "acceptsAccountingExactlyAtEachConfiguredBoundary",
        "rejectsCountKeyBytesAndRevokeCostOverTheConfiguredBoundary",
    },
    "io.etcdhbase.server.M4LeaseBindingRpcIT": {
        "realGoLeaseBindingCallsMatchNativeAndSurviveRestartAcrossLayouts"
    },
    "io.etcdhbase.server.commit.LeaseBindingMutationPlannerTest": {
        "activeStateUpdateIncludesItsCurrentExpiryCandidate",
        "revokedStateUpdatesDoNotWriteNewExpiryCandidates",
    },
    "io.etcdhbase.server.commit.M3OwnerLeaseTest": {
        "startupFormatUpgradeRotatesTheFenceWithoutLosingOwnerRenewal"
    },
    "io.etcdhbase.server.config.M3RuntimeConfigurationTest": {
        "m4ActivationRequiresExplicitLeaseRevocationLimits"
    },
}
J42_GO_TESTS = {
    "cmd/m4driver": {
        "TestRunNativeLeaseKeepAliveWritesFreshTraceAndVerifiableMetadata",
        "TestCompareKeepAliveRejectsCorruptOrMismatchedTrace",
    },
    "internal/scenario": {
        "TestM4LeaseKeepAliveScenarioMatchesItsPinnedNativeOracle",
        "TestLoadM4LeaseKeepAliveSuiteRejectsMissingOrChangedCoverage",
    },
    "internal/compare": {
        "TestCompareM4LeaseKeepAliveRejectsMissingTerminal",
        "TestCompareM4LeaseKeepAliveRejectsChangedRevisionOrUnknownTtl",
    },
    "internal/trace": {
        "TestSnapshotProtoPreservesWatchAndLeaseStreamValues",
    },
}
J42_JAVA_CLASS_MODULES = {
    "io.etcdhbase.core.commit.InvocationResultResolverTest": "core",
    "io.etcdhbase.core.commit.MutationOperationEnvelopeTest": "core",
    "io.etcdhbase.core.request.RequestScopeTest": "core",
    "io.etcdhbase.core.lease.LeaseTimeRulesTest": "core",
    "io.etcdhbase.server.commit.BoundedPublicationHandoffTest": "server",
    "io.etcdhbase.server.commit.LeaseGrantPreparationTest": "server",
    "io.etcdhbase.server.commit.LeaseKeepAliveStreamTest": "server",
    "io.etcdhbase.server.commit.LeaseKeepAliveTimingTest": "server",
    "io.etcdhbase.server.commit.MutationRelayTest": "server",
    "io.etcdhbase.server.commit.M3OwnerLeaseTest": "server",
    "io.etcdhbase.server.commit.MutationForwardReceiverTest": "server",
    "io.etcdhbase.server.commit.OwnerPeerIdentityInterceptorTest": "server",
    "io.etcdhbase.server.commit.SerialMutationQueueTest": "server",
    "io.etcdhbase.server.observability.RequestMetricsTest": "server",
    "io.etcdhbase.server.TestTlsCertificateTest": "server",
    "io.etcdhbase.server.M4LeaseKeepAliveRpcIT": "server",
}
J42_JAVA_CLASSES = set(J42_JAVA_CLASS_MODULES)
J42_CRITICAL_JAVA_TESTS = {
    "io.etcdhbase.core.commit.InvocationResultResolverTest": {
        "resolvesEitherDurableInvocationOutcomeForProtocolProjection",
    },
    "io.etcdhbase.core.commit.MutationOperationEnvelopeTest": {
        "distinguishesVersionedTxnFromExistingRawPutBytes",
    },
    "io.etcdhbase.core.request.RequestScopeTest": {
        "snapshotsLogicalStorageCostsAndRejectsBackwardsDeltas",
        "serviceContinuationSharesStorageAccountingButNotCancellation",
    },
    "io.etcdhbase.core.lease.LeaseTimeRulesTest": {
        "reportsConservativeRemainingTtlInMillisAndCapsAtGrantedTtl",
    },
    "io.etcdhbase.server.commit.BoundedPublicationHandoffTest": {
        "attributesServiceOwnedStorageCostToTheTransferredOperation",
    },
    "io.etcdhbase.server.commit.LeaseKeepAliveStreamTest": {
        "serializesResponsesAndRequestsOneInboundMessageAtATime",
        "doesNotSendUntilReadyAndReleasesPendingDeliveryOnCancellation",
        "preservesGrantedKeepAliveTtlWhenTheNextRenewalWindowStillFits",
        "terminatesInsteadOfPromisingAFullTtlWhenTheClientCannotRenewBeforeExpiry",
        "halfCloseWaitsForCurrentOperationBeforeCompleting",
        "observerFailureReleasesDeliveryAndStopsFurtherInput",
        "timesOutAStalledOutboundResponseAndReleasesItsDelivery",
    },
    "io.etcdhbase.server.commit.LeaseKeepAliveTimingTest": {
        "preservesTheClientScheduleAndResponseSafetyMargin",
        "capsTheMarginForShortTtlsWithoutMakingAResponseWindowImpossible",
        "rejectsNonPositiveOrUnsupportedTtls",
    },
    "io.etcdhbase.server.commit.MutationRelayTest": {
        "staleGatewayOwnerSampleIsRelocatedBeforeAnyRequestIsSent",
        "ownerChangeDuringRouteLookupDiscardsTheStaleRouteAndKeepsInvocation",
        "unknownReplyAfterSendIsNotRelocatedEvenWhenOwnerChanges",
    },
    "io.etcdhbase.server.commit.LeaseGrantPreparationTest": {
        "persistsNoChangeResponseWithoutCreatingAnEmptyAuxiliaryManifest",
    },
    "io.etcdhbase.server.commit.M3OwnerLeaseTest": {
        "standbyStartsReadyAndClaimsOwnerAfterActiveOwnerExpires",
        "samePeerRestartUsesFreshIncarnationAndFencesOldToken",
        "forwardingResolvesFreshAndLegacyOwnerIdentities",
        "standbyClaimTimeoutCancelsStalledRecoveryAndAllowsRetry",
    },
    "io.etcdhbase.server.commit.MutationForwardReceiverTest": {
        "staleSealedBindingWithoutDurableSelectionIsResolvedWithoutMutation",
        "staleSealedBindingAfterSelectionCanReturnItsDurableOutcome",
    },
    "io.etcdhbase.server.commit.SerialMutationQueueTest": {
        "zeroQueuedCapacityRejectsConcurrentRequestsWithoutDisplacingTheActiveMutation",
    },
    "io.etcdhbase.server.commit.OwnerPeerIdentityInterceptorTest": {
        "acceptsOnlyOneConfiguredDnsSan",
        "rejectsUnregisteredDnsSanEvenWhenItsCertificateIsTrusted",
        "rejectsCertificatesWithoutDnsSanInsteadOfFallingBackToCommonName",
        "rejectsCertificatesWithMultipleDnsSansEvenWhenBothAreConfigured",
    },
    "io.etcdhbase.server.TestTlsCertificateTest": {
        "createsCertificatesWithNoAndMultipleDnsNames",
    },
    "io.etcdhbase.server.observability.RequestMetricsTest": {
        "snapshotsBoundedKeepAliveStagesAndLogicalPayloadBytes",
    },
    "io.etcdhbase.server.M4LeaseKeepAliveRpcIT": {
        "realGoKeepAliveMatchesNativeAcrossLayouts",
        "expiredLeaseIsDurablyRevokedBeforeKeepAliveRetry",
        "dualRuntimeForwardsKeepAliveOverMtlsAndFencesTheOldOwner",
        "dualProcessKeepAliveSurvivesOwnerFailureAndFencesStaleEpoch",
        "persistedKeepAliveSurvivesLostOwnerReplyAndRestart",
        "persistedKeepAliveSurvivesClientCancellationBeforeResponse",
    },
}


class ReportError(ValueError):
    """Raised when a run cannot prove its registered M4 requirements."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReportError("duplicate JSON object key")
        result[key] = value
    return result


def read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {path.name}") from error


def validate_registry_document(document: object) -> dict[str, object]:
    if not isinstance(document, dict) or document.get("schema") != "etcd-hbase-m4-cases/v1":
        raise ReportError("M4 registry schema is invalid")
    if document.get("protocol") != PROTOCOL:
        raise ReportError("M4 registry protocol is not pinned")
    tasks = document.get("tasks")
    pending = document.get("pending_tasks")
    if not isinstance(tasks, list) or not isinstance(pending, list):
        raise ReportError("M4 registry tasks and pending_tasks must be lists")

    task_map: dict[str, dict[str, object]] = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise ReportError("M4 registry task is malformed")
        task_id = task.get("id")
        if not isinstance(task_id, str) or task_id not in KNOWN_TASKS or task_id in task_map:
            raise ReportError("M4 registry task ID is unknown or duplicated")
        _validate_task_shape(task)
        task_map[task_id] = task
    if any(not isinstance(item, str) for item in pending):
        raise ReportError("M4 pending task list is malformed")
    pending_ids = set(pending)
    if len(pending_ids) != len(pending):
        raise ReportError("M4 pending task list is malformed or duplicated")
    if task_map.keys() & pending_ids or set(task_map) | pending_ids != KNOWN_TASKS:
        raise ReportError("M4 task registration and pending list do not cover the plan")

    j40 = task_map.get("J40")
    if j40 is None:
        raise ReportError("J40 evidence registration is required")
    _validate_j40_registration(j40)
    j41 = task_map.get("J41")
    if j41 is not None:
        _validate_j41_registration(j41)
    j42 = task_map.get("J42")
    if j42 is not None:
        _validate_j42_registration(j42)
    j43 = task_map.get("J43")
    if j43 is not None:
        _validate_j43_registration(j43)
    j44 = task_map.get("J44")
    if j44 is not None:
        _validate_j44_registration(j44)
    j45 = task_map.get("J45")
    if j45 is not None:
        _validate_j45_registration(j45)
    j46 = task_map.get("J46")
    if j46 is not None:
        _validate_j46_registration(j46)
    j47 = task_map.get("J47")
    if j47 is not None:
        _validate_j47_registration(j47)
    return document


def _validate_task_shape(task: dict[str, object]) -> None:
    if task.get("id") == "J44":
        if (task.get("kind") != "event-storage"
                or not isinstance(task.get("maven_tests"), list)
                or not task["maven_tests"]):
            raise ReportError("J44 must register its event-storage integration suites")
        return
    if task.get("id") == "J45":
        if (task.get("kind") != "watch-stream"
                or not isinstance(task.get("maven_tests"), list)
                or not task["maven_tests"]
                or not isinstance(task.get("go_tests"), list)
                or not isinstance(task.get("trace_cases"), list)
                or not isinstance(task.get("watch_start_matrix"), list)
                or task.get("verification_boundary") != J45_VERIFICATION_BOUNDARY):
            raise ReportError("J45 must register its raw-proto oracle and local Watch suites")
        return
    if task.get("id") == "J46":
        if (task.get("kind") != "watch-replay"
                or not isinstance(task.get("maven_tests"), list)
                or not task["maven_tests"]
                or not isinstance(task.get("go_tests"), list)
                or not task["go_tests"]
                or task.get("verification_boundary") != J46_VERIFICATION_BOUNDARY):
            raise ReportError("J46 must register replay/progress suites and its verification boundary")
        return
    if task.get("id") == "J47":
        if (task.get("kind") != "watch-readiness"
                or not isinstance(task.get("maven_tests"), list)
                or not task["maven_tests"]
                or not isinstance(task.get("go_tests"), list)
                or task.get("verification_boundary") != J47_VERIFICATION_BOUNDARY):
            raise ReportError("J47 must register Watch readiness suites and its verification boundary")
        return
    if not isinstance(task.get("api_ids"), list) or not task["api_ids"]:
        raise ReportError("registered M4 task must identify API coverage")
    if not isinstance(task.get("scenario"), str) or not task["scenario"]:
        raise ReportError("registered M4 task must identify its scenario")
    for field in ("trace_cases", "go_tests", "maven_tests"):
        if not isinstance(task.get(field), list) or not task[field]:
            raise ReportError(f"registered M4 task must have {field}")
    evidence = task.get("evidence")
    if not isinstance(evidence, dict) or not isinstance(evidence.get("native"), dict):
        raise ReportError("registered M4 task must describe native evidence")
    if not isinstance(evidence.get("targets"), list) or not evidence["targets"]:
        raise ReportError("registered M4 task must describe target evidence")
    if not isinstance(evidence.get("compare_logs"), list) or not evidence["compare_logs"]:
        raise ReportError("registered M4 task must require comparison evidence")


def _validate_j40_registration(task: dict[str, object]) -> None:
    _validate_j40_api_and_trace(task)
    _validate_j40_go_tests(task["go_tests"])
    _validate_j40_java_tests(task["maven_tests"])
    _validate_j40_evidence(task["evidence"])


def _validate_j40_api_and_trace(task: dict[str, object]) -> None:
    if (task.get("api_ids") != ["L01", "L04", "L05"]
            or task.get("scenario") != "m4-lease-v1"
            or task.get("manifest") != "m4-lease.yaml"):
        raise ReportError("J40 API or scenario registration changed")
    traces = task["trace_cases"]
    if any(not isinstance(item, dict) for item in traces):
        raise ReportError("J40 Lease trace case is malformed")
    if [(item.get("id"), item.get("method")) for item in traces] != list(J40_TRACE_CASES):
        raise ReportError("J40 Lease trace coverage is missing, reordered, or changed")
    if any(not isinstance(item.get("api_id"), str) or not item["api_id"] for item in traces):
        raise ReportError("J40 trace case has no API mapping")


def _registered_go_tests(registrations: list[dict[str, object]], label: str) -> dict[str, set[str]]:
    go_tests: dict[str, set[str]] = {}
    for package in registrations:
        if not isinstance(package, dict) or not isinstance(package.get("package"), str):
            raise ReportError(f"{label} Go test registration is malformed")
        names = package.get("tests")
        if not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names):
            raise ReportError(f"{label} Go package has no named required tests")
        package_name = package["package"]
        if package_name in go_tests:
            raise ReportError(f"{label} Go package is registered more than once")
        go_tests[package_name] = set(names)
    return go_tests


def _validate_j40_go_tests(registrations: list[dict[str, object]]) -> None:
    go_tests = _registered_go_tests(registrations, "M4")
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J40_GO_TESTS.items()):
        raise ReportError("J40 Go regression coverage is incomplete")


def _registered_java_suite_index(
    registrations: list[dict[str, object]], label: str, *,
    require_single_module: bool = False, required_kind: str | None = None,
) -> tuple[set[str], dict[str, str], dict[str, set[str]]]:
    java_classes: set[str] = set()
    java_modules: dict[str, str] = {}
    java_methods: dict[str, set[str]] = {}
    for suite in registrations:
        if not isinstance(suite, dict):
            raise ReportError(f"{label} Maven test registration is malformed")
        module, kind, class_name, tests = (
            suite.get("module"), suite.get("kind"), suite.get("class"), suite.get("tests")
        )
        if module not in JAVA_MODULES or kind not in JAVA_KINDS or not isinstance(class_name, str):
            raise ReportError(f"{label} Maven test registration has an invalid module, kind, or class")
        if required_kind is not None and kind != required_kind:
            raise ReportError(f"{label} Maven suites must be JVM unit tests in known modules")
        if not isinstance(tests, list) or not tests or any(not isinstance(name, str) for name in tests):
            raise ReportError(f"{label} Maven suite has no named required tests")
        java_classes.add(class_name)
        previous_module = java_modules.setdefault(class_name, module)
        if require_single_module and previous_module != module:
            raise ReportError(f"{label} Maven suite is registered in multiple modules")
        java_methods.setdefault(class_name, set()).update(tests)
    return java_classes, java_modules, java_methods


def _registered_java_tests(registrations: list[dict[str, object]], label: str) -> dict[str, set[str]]:
    _, _, java_methods = _registered_java_suite_index(registrations, label)
    return java_methods


def _validate_j40_java_tests(registrations: list[dict[str, object]]) -> None:
    java_methods = _registered_java_tests(registrations, "M4")
    if not J40_JAVA_CLASSES.issubset(java_methods):
        raise ReportError("J40 Maven unit or MiniCluster suites are not all registered")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J40_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J40 clock, binding, TTL projection, bounded-scan, or real RPC test is not registered")


def _validate_j40_evidence(evidence: dict[str, object]) -> None:
    native = evidence["native"]
    targets = evidence["targets"]
    if (native.get("scope"), native.get("layout"), native.get("embedded_native")) != (
        "native-etcd", "", True
    ):
        raise ReportError("J40 native evidence is not an isolated embedded etcd run")
    target_layouts = {
        (target.get("scope"), target.get("layout"), target.get("embedded_native"))
        for target in targets if isinstance(target, dict)
    }
    if target_layouts != {
        ("target-hbase", "HISTORY", False), ("target-hbase", "NATIVE", False)
    }:
        raise ReportError("J40 must compare both real HBase layouts")
    if evidence.get("compare_logs") != ["compare-history.log", "compare-native.log"]:
        raise ReportError("J40 comparison log registration changed")


def _validate_j41_registration(task: dict[str, object]) -> None:
    _validate_j41_api_and_trace(task)
    _validate_j41_go_tests(task["go_tests"])
    _validate_j41_java_tests(task["maven_tests"])
    _validate_j41_evidence(task["evidence"])


def _validate_j41_api_and_trace(task: dict[str, object]) -> None:
    if (task.get("api_ids") != ["L01", "L04", "L05", "P01", "T01", "D01", "R01"]
            or task.get("scenario") != "m4-lease-bindings-v1"
            or task.get("manifest") != "m4-lease-bindings.yaml"
            or task.get("artifact_subdir") != "j41"):
        raise ReportError("J41 API or scenario registration changed")
    traces = task["trace_cases"]
    if any(not isinstance(item, dict) for item in traces):
        raise ReportError("J41 binding trace case is malformed")
    if [(item.get("id"), item.get("method")) for item in traces] != list(J41_TRACE_CASES):
        raise ReportError("J41 lease-binding trace coverage is missing, reordered, or changed")
    if any(not isinstance(item.get("api_id"), str) or not item["api_id"] for item in traces):
        raise ReportError("J41 trace case has no API mapping")


def _validate_j41_go_tests(registrations: list[dict[str, object]]) -> None:
    go_tests = _registered_go_tests(registrations, "J41")
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J41_GO_TESTS.items()):
        raise ReportError("J41 Go binding regression coverage is incomplete")


def _validate_j41_java_tests(registrations: list[dict[str, object]]) -> None:
    java_methods = _registered_java_tests(registrations, "J41")
    if not J41_JAVA_CLASSES.issubset(java_methods):
        raise ReportError("J41 Maven unit or MiniCluster suites are not all registered")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J41_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J41 binding budget, restart, or atomic registration regression is not registered")


def _validate_j41_evidence(evidence: dict[str, object]) -> None:
    native = evidence["native"]
    target_layouts = {
        (target.get("scope"), target.get("layout"), target.get("embedded_native"))
        for target in evidence["targets"] if isinstance(target, dict)
    }
    if (native.get("scope"), native.get("layout"), native.get("embedded_native")) != (
        "native-etcd", "", True
    ) or target_layouts != {
        ("target-hbase", "HISTORY", False), ("target-hbase", "NATIVE", False)
    }:
        raise ReportError("J41 must compare native etcd with both HBase layouts")
    if evidence.get("compare_logs") != ["compare-history.log", "compare-native.log"]:
        raise ReportError("J41 comparison log registration changed")


def _validate_j42_registration(task: dict[str, object]) -> None:
    if (task.get("api_ids") != ["L01", "L02", "L04", "L05", "R01"]
            or task.get("scenario") != "m4-lease-keepalive-v1"
            or task.get("manifest") != "m4-lease-keepalive.yaml"
            or task.get("artifact_subdir") != "j42"):
        raise ReportError("J42 API, scenario, or evidence directory changed")
    traces = task["trace_cases"]
    if any(not isinstance(item, dict) for item in traces):
        raise ReportError("J42 KeepAlive trace case is malformed")
    if [(item.get("id"), item.get("method"), item.get("api_id")) for item in traces] != list(J42_TRACE_CASES):
        raise ReportError("J42 KeepAlive trace coverage is missing, reordered, or changed")
    registered_terminals = {
        item["id"]: item["stream_terminal"]
        for item in traces if item.get("stream_terminal") is not None
    }
    if registered_terminals != J42_STREAM_TERMINALS:
        raise ReportError("J42 stream terminal coverage is missing or changed")

    go_tests: dict[str, set[str]] = {}
    for package in task["go_tests"]:
        if not isinstance(package, dict) or not isinstance(package.get("package"), str):
            raise ReportError("J42 Go test registration is malformed")
        names = package.get("tests")
        if not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names):
            raise ReportError("J42 Go package has no named required tests")
        if package["package"] in go_tests:
            raise ReportError("J42 Go package is registered more than once")
        go_tests[package["package"]] = set(names)
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J42_GO_TESTS.items()):
        raise ReportError("J42 Go KeepAlive regression coverage is incomplete")

    java_classes, java_modules, java_methods = _registered_java_suite_index(
        task["maven_tests"], "J42", require_single_module=True
    )
    if not J42_JAVA_CLASSES.issubset(java_classes):
        raise ReportError("J42 stream, standby-owner, or real dual-runtime suites are not registered")
    if any(java_modules.get(class_name) != module
           for class_name, module in J42_JAVA_CLASS_MODULES.items()):
        raise ReportError("J42 Java suites must be collected from their declared modules")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J42_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J42 stream backpressure, owner failover, or persistence tests are not registered")

    evidence = task["evidence"]
    native = evidence["native"]
    target_layouts = {
        (target.get("scope"), target.get("layout"), target.get("embedded_native"))
        for target in evidence["targets"] if isinstance(target, dict)
    }
    if (native.get("scope"), native.get("layout"), native.get("embedded_native")) != (
        "native-etcd", "", True
    ) or target_layouts != {
        ("target-hbase", "HISTORY", False), ("target-hbase", "NATIVE", False)
    }:
        raise ReportError("J42 must compare native KeepAlive with both HBase layouts")
    if evidence.get("compare_logs") != ["keepalive-history.log", "keepalive-native.log"]:
        raise ReportError("J42 comparison log registration changed")


def _validate_j43_registration(task: dict[str, object]) -> None:
    if (task.get("api_ids") != ["L01", "L03", "L04", "L05", "P01", "R01"]
            or task.get("scenario") != "m4-lease-revoke-v1"
            or task.get("manifest") != "m4-lease-revoke.yaml"
            or task.get("artifact_subdir") != "j43"):
        raise ReportError("J43 API, scenario, or evidence directory changed")
    traces = task["trace_cases"]
    if any(not isinstance(item, dict) for item in traces):
        raise ReportError("J43 Revoke trace case is malformed")
    if [(item.get("id"), item.get("method"), item.get("api_id")) for item in traces] != list(J43_TRACE_CASES):
        raise ReportError("J43 Revoke and expiry trace coverage is missing, reordered, or changed")

    go_tests: dict[str, set[str]] = {}
    for package in task["go_tests"]:
        if not isinstance(package, dict) or not isinstance(package.get("package"), str):
            raise ReportError("J43 Go test registration is malformed")
        names = package.get("tests")
        if not isinstance(names, list) or not names or any(not isinstance(name, str) for name in names):
            raise ReportError("J43 Go package has no named required tests")
        if package["package"] in go_tests:
            raise ReportError("J43 Go package is registered more than once")
        go_tests[package["package"]] = set(names)
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J43_GO_TESTS.items()):
        raise ReportError("J43 Go Revoke regression coverage is incomplete")

    java_classes, java_modules, java_methods = _registered_java_suite_index(
        task["maven_tests"], "J43", require_single_module=True
    )
    if not J43_JAVA_CLASSES.issubset(java_classes):
        raise ReportError("J43 expiry, Revoke, race, or mixed-load suites are not registered")
    if any(java_modules.get(class_name) != module
           for class_name, module in J43_JAVA_CLASS_MODULES.items()):
        raise ReportError("J43 Java suites must be collected from their declared modules")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J43_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J43 checkpoint, clock, race, or mixed-load regression coverage is incomplete")

    evidence = task["evidence"]
    native = evidence["native"]
    target_layouts = {
        (target.get("scope"), target.get("layout"), target.get("embedded_native"))
        for target in evidence["targets"] if isinstance(target, dict)
    }
    if (native.get("scope"), native.get("layout"), native.get("embedded_native")) != (
        "native-etcd", "", True
    ) or target_layouts != {
        ("target-hbase", "HISTORY", False), ("target-hbase", "NATIVE", False)
    }:
        raise ReportError("J43 must compare native Lease Revoke with both HBase layouts")
    if evidence.get("compare_logs") != ["revoke-history.log", "revoke-native.log"]:
        raise ReportError("J43 comparison log registration changed")


def _validate_j44_registration(task: dict[str, object]) -> None:
    if task.get("kind") != "event-storage" or task.get("id") != "J44":
        raise ReportError("J44 event-storage registration changed")
    java_classes, java_modules, java_methods = _registered_java_suite_index(
        task["maven_tests"], "J44", require_single_module=True
    )
    if java_classes != set(J44_JAVA_CLASS_MODULES):
        raise ReportError("J44 cursor, pin, backfill, shared tailer, and MiniCluster suites are incomplete")
    if any(java_modules.get(class_name) != module
           for class_name, module in J44_JAVA_CLASS_MODULES.items()):
        raise ReportError("J44 Java suites must be collected from their declared modules")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J44_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J44 event cursor, backfill, fairness, or layout regression coverage is incomplete")


def _validate_j45_registration(task: dict[str, object]) -> None:
    _validate_j45_task_registration(task)
    _validate_j45_trace_registration(task)
    _validate_j45_watch_matrix_registration(task)
    _validate_j45_go_evidence_registration(task)
    _validate_j45_java_registration(task)


def _validate_j45_task_registration(task: dict[str, object]) -> None:
    if task.get("kind") != "watch-stream" or task.get("id") != "J45":
        raise ReportError("J45 Watch stream registration changed")
    if (task.get("scenario") != "m4-watch-starts-v1"
            or task.get("manifest") != "m4-watch-starts.yaml"
            or task.get("artifact_subdir") != "j45"
            or task.get("api_ids") != ["C14", "C19", "P01", "R01"]):
        raise ReportError("J45 raw-proto Watch scenario registration changed")


def _validate_j45_trace_registration(task: dict[str, object]) -> None:
    traces = task["trace_cases"]
    if (any(not isinstance(item, dict) for item in traces)
            or [(item.get("id"), item.get("method")) for item in traces] != list(J45_TRACE_CASES)):
        raise ReportError("J45 native Watch trace is missing, reordered, or changed")
    api_id_by_case = {
        case_id: _j45_trace_api_id(case_id)
        for case_id, _ in J45_TRACE_CASES
    }
    if any(item.get("api_id") != api_id_by_case[item["id"]] for item in traces):
        raise ReportError("J45 trace case API mappings changed")


def _j45_trace_api_id(case_id: str) -> str:
    if case_id in {
        "watch-start-history-put", "watch-negative-live-put", "watch-compact-fixture-put",
    }:
        return "P01"
    if case_id == "watch-compact-fixture-range":
        return "R01"
    if case_id == "watch-compact-fixture-compact":
        return "C14"
    return "C19"


def _validate_j45_watch_matrix_registration(task: dict[str, object]) -> None:
    matrix = task["watch_start_matrix"]
    matrix_fields = ("id", "state", "start_revision", "compacted", "compact_revision")
    if (any(not isinstance(item, dict) for item in matrix)
            or [tuple(item.get(field) for field in matrix_fields) for item in matrix]
            != list(J45_START_MATRIX)):
        raise ReportError("J45 must register initial -2/-1/0/future and compacted -2/-1/0 Watch starts")
    watch_id_matrix = task.get("watch_id_matrix")
    watch_id_fields = ("id", "state", "watch_id", "start_revision")
    if (not isinstance(watch_id_matrix, list)
            or any(not isinstance(item, dict) for item in watch_id_matrix)
            or [tuple(item.get(field) for field in watch_id_fields) for item in watch_id_matrix]
            != list(J45_RAW_WATCH_ID_MATRIX)):
        raise ReportError("J45 must register raw Watch ID -1 create and cancel in both phases")


def _validate_j45_go_evidence_registration(task: dict[str, object]) -> None:
    go_tests = _registered_go_tests(task["go_tests"], "J45")
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J45_GO_TESTS.items()):
        raise ReportError("J45 raw-proto native oracle tests are incomplete")
    evidence = task.get("evidence")
    if (not isinstance(evidence, dict) or set(evidence) != {"native", "local_service"}
            or evidence.get("native") != {
                "scope": "native-etcd", "layout": "", "embedded_native": True,
            } or evidence.get("local_service") != {
                "scope": "local-service", "layout": "", "embedded_native": False,
            }):
        raise ReportError("J45 must register native and local Java WatchService evidence")


def _validate_j45_java_registration(task: dict[str, object]) -> None:
    java_classes, java_modules, java_methods = _registered_java_suite_index(
        task["maven_tests"], "J45", require_single_module=True, required_kind="surefire"
    )
    if java_classes != set(J45_JAVA_CLASS_MODULES):
        raise ReportError("J45 tailer, RPC wiring, stream, and gateway suites are incomplete")
    if any(java_modules.get(class_name) != module
           for class_name, module in J45_JAVA_CLASS_MODULES.items()):
        raise ReportError("J45 Java suites must be collected from their declared modules")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J45_CRITICAL_JAVA_TESTS.items()):
        raise ReportError(
            "J45 revision floors, termination/resource cleanup, or single-writer coverage is incomplete"
        )


def _validate_j46_registration(task: dict[str, object]) -> None:
    if task.get("kind") != "watch-replay" or task.get("id") != "J46":
        raise ReportError("J46 Watch replay registration changed")
    if (task.get("api_ids") != ["C19"]
            or task.get("verification_boundary") != J46_VERIFICATION_BOUNDARY):
        raise ReportError("J46 Watch replay/progress scope or verification boundary changed")
    go_tests = _registered_go_tests(task["go_tests"], "J46")
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J46_GO_TESTS.items()):
        raise ReportError("J46 pinned ProgressRequest native oracle is incomplete")
    java_methods: dict[str, set[str]] = {}
    java_modules: dict[str, str] = {}
    for suite in task["maven_tests"]:
        if not isinstance(suite, dict):
            raise ReportError("J46 Maven test registration is malformed")
        module, kind, class_name, tests = (
            suite.get("module"), suite.get("kind"), suite.get("class"), suite.get("tests")
        )
        if not isinstance(class_name, str):
            raise ReportError("J46 Maven test class name is malformed")
        expected_kind = J46_JAVA_CLASS_KINDS.get(class_name)
        if module not in JAVA_MODULES or kind != expected_kind:
            raise ReportError("J46 Maven suites must match the registered unit/integration suites")
        if not isinstance(tests, list) or not tests or any(not isinstance(name, str) for name in tests):
            raise ReportError("J46 Maven suite has no named required tests")
        previous_module = java_modules.setdefault(class_name, module)
        if previous_module != module:
            raise ReportError("J46 Maven suite is registered in multiple modules")
        java_methods.setdefault(class_name, set()).update(tests)
    if set(java_modules) != set(J46_JAVA_CLASS_MODULES):
        raise ReportError("J46 replay, progress, fragment, or Go-client integration suites are incomplete")
    if any(java_modules.get(class_name) != module
           for class_name, module in J46_JAVA_CLASS_MODULES.items()):
        raise ReportError("J46 Java suites must be collected from their declared modules")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J46_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J46 replay ordering, handoff, or progress barrier coverage is incomplete")


def _validate_j47_registration(task: dict[str, object]) -> None:
    if (task.get("kind") != "watch-readiness" or task.get("id") != "J47"
            or task.get("api_ids") != ["C19"]
            or task.get("verification_boundary") != J47_VERIFICATION_BOUNDARY):
        raise ReportError("J47 Watch readiness scope or verification boundary changed")
    go_tests = _registered_go_tests(task["go_tests"], "J47")
    if any(not expected.issubset(go_tests.get(package, set()))
           for package, expected in J47_GO_TESTS.items()):
        raise ReportError("J47 pinned etcd v3.5.23 half-close oracle is incomplete")

    java_methods: dict[str, set[str]] = {}
    java_modules: dict[str, str] = {}
    for suite in task["maven_tests"]:
        if not isinstance(suite, dict):
            raise ReportError("J47 Maven test registration is malformed")
        module, kind, class_name, tests = (
            suite.get("module"), suite.get("kind"), suite.get("class"), suite.get("tests")
        )
        if not isinstance(class_name, str):
            raise ReportError("J47 Maven test class name is malformed")
        expected_kind = J47_JAVA_CLASS_KINDS.get(class_name)
        if module not in JAVA_MODULES or kind != expected_kind:
            raise ReportError("J47 Maven suites must match registered unit/integration suites")
        if not isinstance(tests, list) or not tests or any(not isinstance(name, str) for name in tests):
            raise ReportError("J47 Maven suite has no named required tests")
        previous_module = java_modules.setdefault(class_name, module)
        if previous_module != module:
            raise ReportError("J47 Maven suite is registered in multiple modules")
        java_methods.setdefault(class_name, set()).update(tests)
    if set(java_modules) != set(J47_JAVA_CLASS_MODULES):
        raise ReportError("J47 stream cleanup or MiniCluster integration suites are incomplete")
    if any(java_modules.get(class_name) != module
           for class_name, module in J47_JAVA_CLASS_MODULES.items()):
        raise ReportError("J47 Java suites must be collected from their declared modules")
    if any(not methods.issubset(java_methods.get(class_name, set()))
           for class_name, methods in J47_CRITICAL_JAVA_TESTS.items()):
        raise ReportError("J47 cancellation, backpressure, compaction, or cleanup coverage is incomplete")


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, object]:
    return validate_registry_document(read_json(path))


def validate_all_tasks_registered(registry: dict[str, object]) -> None:
    pending = registry.get("pending_tasks")
    if pending:
        raise ReportError("--all is unavailable while M4 tasks are not registered")
    if {task["id"] for task in registry["tasks"]} != KNOWN_TASKS:
        raise ReportError("--all requires runnable registrations for every M4 task")


def artifact_location(repository: Path, requested: str, *, require_existing: bool = True) -> Path:
    root = repository.resolve()
    raw = Path(requested)
    candidate = Path(os.path.abspath(raw if raw.is_absolute() else root / raw))
    allowed = root / ".artifacts" / "m4"
    try:
        relative = candidate.relative_to(allowed)
    except ValueError as error:
        raise ReportError("M4 artifact directory must be below .artifacts/m4") from error
    if not relative.parts or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", candidate.name):
        raise ReportError("M4 artifact directory must have a fresh named run ID")
    current = root
    for component in (".artifacts", "m4", *relative.parts):
        current = current / component
        if current.is_symlink():
            raise ReportError("M4 artifact path must not traverse a symbolic link")
    if require_existing and not candidate.is_dir():
        raise ReportError("M4 artifact directory does not exist")
    return candidate


def validate_repository_binding(repository: Path, run: dict[str, object]) -> None:
    def git(*arguments: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(repository), *arguments], text=True, stderr=subprocess.DEVNULL
        ).strip()

    try:
        head = git("rev-parse", "HEAD")
        dirty = bool(git("status", "--porcelain", "--untracked-files=normal"))
    except (OSError, subprocess.CalledProcessError) as error:
        raise ReportError("cannot verify the source checkout for this M4 run") from error
    if head != run["source_commit"] or head != run["checkout"]:
        raise ReportError("M4 run source SHA does not match the current checkout")
    if run["worktree_clean"] and dirty:
        raise ReportError("clean M4 evidence no longer has a clean source tree")


def _validate_commands(artifact: Path, run: dict[str, object], task: dict[str, object]) -> None:
    commands = run.get("commands")
    if not isinstance(commands, list) or not commands:
        raise ReportError("M4 run has no command evidence")
    names: set[str] = set()
    for command in commands:
        if not isinstance(command, dict):
            raise ReportError("M4 command record is malformed")
        name = command.get("name")
        argv = command.get("argv")
        if not isinstance(name, str) or name in names or not isinstance(argv, list) or not argv:
            raise ReportError("M4 command name or argv is missing or duplicated")
        names.add(name)
        if command.get("exit_code") != 0:
            raise ReportError(f"M4 command {name} did not exit successfully")
        for field in ("started_at", "finished_at"):
            if not isinstance(command.get(field), str) or not command[field]:
                raise ReportError(f"M4 command {name} has no {field}")
        for field in ("stdout_log", "stderr_log"):
            log_name = command.get(field)
            if log_name is None:
                continue
            if not isinstance(log_name, str):
                raise ReportError(f"M4 command {name} has an invalid {field}")
            log_path = Path(os.path.abspath(artifact / log_name))
            try:
                log_path.relative_to(artifact)
            except ValueError as error:
                raise ReportError("M4 command log path escapes its artifact directory") from error
            if not log_path.is_file() or log_path.is_symlink():
                raise ReportError(f"M4 command log for {name} is missing or unsafe")
    required = {
        "J44": J44_REQUIRED_COMMANDS,
        "J45": J45_REQUIRED_COMMANDS,
        "J46": J46_REQUIRED_COMMANDS,
        "J47": J47_REQUIRED_COMMANDS,
    }.get(task.get("id"), REQUIRED_COMMANDS)
    task_command_names = names - {"validate-artifact"}
    if not required.issubset(names):
        raise ReportError("M4 run is missing a required task verification command")
    if task.get("id") == "J45":
        if task_command_names != J45_REQUIRED_COMMANDS:
            raise ReportError("J45 verification must run its native Go oracle and JVM unit profile only")
        maven = next(command for command in commands if command["name"] == "maven-unit-tests")
        if ("-Pm4-j45-public" not in maven["argv"] or "test" not in maven["argv"]
                or "verify" in maven["argv"]):
            raise ReportError("J45 Maven verification must run Surefire only with the local profile")
        go_tests = next(command for command in commands if command["name"] == "go-tests")
        if ("scripts/dev/go-m0.sh" not in go_tests["argv"] or "test" not in go_tests["argv"]
                or "-json" not in go_tests["argv"] or "-run" not in go_tests["argv"]
                or "./internal/scenario" not in go_tests["argv"]
                or "./cmd/m4driver" not in go_tests["argv"]
                or "./internal/trace" not in go_tests["argv"]):
            raise ReportError("J45 Go tests must run the registered native and local-service Watch suites")
        oracle = next(command for command in commands if command["name"] == "native-watch-oracle")
        argv = oracle["argv"]
        required_argv = (
            "scripts/dev/go-m0.sh", "run", "./cmd/m4driver", "run", "--scenario",
            "../scenarios/m4-watch-starts.yaml", "--native-data-dir", "--scope",
            "native-etcd", "--prefix", f"etcd-hbase-m4-test/{run['run_id']}/watch/",
            "--trace", "--metadata",
        )
        if any(value not in argv for value in required_argv):
            raise ReportError("J45 native oracle command does not run the pinned raw-proto scenario")
        for flag in ("--native-data-dir", "--trace", "--metadata"):
            value = argv[argv.index(flag) + 1] if argv.index(flag) + 1 < len(argv) else ""
            resolved = Path(os.path.abspath(value))
            try:
                resolved.relative_to(artifact)
            except ValueError as error:
                raise ReportError("J45 native oracle outputs must stay inside the artifact directory") from error
            if flag != "--native-data-dir" and (not resolved.is_file() or resolved.is_symlink()):
                raise ReportError("J45 native oracle trace or metadata is missing or unsafe")
    if task.get("id") == "J46":
        if task_command_names != J46_REQUIRED_COMMANDS:
            raise ReportError("J46 verification must run its pinned Go oracle and MiniCluster profile")
        go_tests = next(command for command in commands if command["name"] == "go-tests")
        go_test_pattern = (
            go_tests["argv"][go_tests["argv"].index("-run") + 1]
            if "-run" in go_tests["argv"] and go_tests["argv"].index("-run") + 1 < len(go_tests["argv"])
            else ""
        )
        if ("scripts/dev/go-m0.sh" not in go_tests["argv"] or "test" not in go_tests["argv"]
                or "-json" not in go_tests["argv"] or "-run" not in go_tests["argv"]
                or "./internal/scenario" not in go_tests["argv"]
                or any(test_name not in go_test_pattern
                       for tests in J46_GO_TESTS.values() for test_name in tests)):
            raise ReportError("J46 Go tests must run the registered etcd v3.5.23 raw-proto progress oracle")
        maven = next(command for command in commands if command["name"] == "maven-verify")
        if ("-Pm4-j46-public" not in maven["argv"] or "verify" not in maven["argv"]
                or J46_MAVEN_TEST_SELECTOR not in maven["argv"]
                or J46_MAVEN_IT_SELECTOR not in maven["argv"]
                or J46_FAILSAFE_EMPTY_MODULES not in maven["argv"]
                or "-Dsurefire.failIfNoSpecifiedTests=false" not in maven["argv"]):
            raise ReportError("J46 Maven verification must run registered Surefire and Failsafe suites")
    if task.get("id") == "J47":
        if task_command_names != J47_REQUIRED_COMMANDS:
            raise ReportError("J47 verification must run its half-close oracle and MiniCluster profile")
        go_tests = next(command for command in commands if command["name"] == "go-tests")
        go_test_pattern = (
            go_tests["argv"][go_tests["argv"].index("-run") + 1]
            if "-run" in go_tests["argv"]
            and go_tests["argv"].index("-run") + 1 < len(go_tests["argv"])
            else ""
        )
        if ("scripts/dev/go-m0.sh" not in go_tests["argv"] or "test" not in go_tests["argv"]
                or "-json" not in go_tests["argv"] or "-run" not in go_tests["argv"]
                or "./internal/scenario" not in go_tests["argv"]
                or any(test_name not in go_test_pattern
                       for tests in J47_GO_TESTS.values() for test_name in tests)):
            raise ReportError("J47 Go tests must run the pinned etcd v3.5.23 half-close oracle")
        maven = next(command for command in commands if command["name"] == "maven-verify")
        if ("-Pm4-j47-public" not in maven["argv"] or "verify" not in maven["argv"]
                or J47_MAVEN_TEST_SELECTOR not in maven["argv"]
                or J47_MAVEN_IT_SELECTOR not in maven["argv"]
                or J47_FAILSAFE_EMPTY_MODULES not in maven["argv"]
                or "-Dsurefire.failIfNoSpecifiedTests=false" not in maven["argv"]):
            raise ReportError("J47 Maven verification must run registered unit and MiniCluster suites")


def _validate_go_tests(artifact: Path, task: dict[str, object]) -> int:
    path = artifact / "go-test.jsonl"
    try:
        if path.stat().st_size > 128 << 20:
            raise ReportError("M4 Go test log exceeds its size limit")
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise ReportError("M4 Go test JSONL is missing or unreadable") from error
    required = {item["package"]: set(item["tests"]) for item in task["go_tests"]}
    observed_packages: set[str] = set()
    started_packages: set[str] = set()
    passed: dict[str, set[str]] = {}
    event_count = 0
    for line_number, line in enumerate(lines, start=1):
        event = _parse_go_test_event(line, line_number)
        action, package, test_name = _validate_go_test_event(event, required)
        event_count += 1
        _record_go_test_event(action, package, test_name, started_packages, observed_packages, passed)
    _validate_go_test_completion(required, started_packages, observed_packages, passed, event_count)
    return sum(len(tests) for tests in passed.values())


def _parse_go_test_event(line: str, line_number: int) -> dict[str, object]:
    if not line or len(line.encode("utf-8")) > 2 << 20:
        raise ReportError(f"M4 Go test JSONL line {line_number} is empty or oversized")
    try:
        event = json.loads(line, object_pairs_hook=_unique_object)
    except (json.JSONDecodeError, ReportError) as error:
        raise ReportError(f"M4 Go test JSONL line {line_number} is invalid") from error
    if not isinstance(event, dict):
        raise ReportError("M4 Go test JSONL contains a non-object event")
    return event


def _validate_go_test_event(
    event: dict[str, object], required: dict[str, set[str]],
) -> tuple[str, str, object]:
    action = event.get("Action")
    package = event.get("Package")
    test_name = event.get("Test")
    if not isinstance(action, str):
        raise ReportError("M4 Go test JSONL contains an unknown action")
    if action in {"fail", "skip"}:
        raise ReportError("M4 Go tests contain a failure or skip")
    if action not in {"start", "run", "output", "pass"}:
        raise ReportError("M4 Go test JSONL contains an unknown action")
    if not isinstance(package, str) or not package.startswith(GO_MODULE):
        raise ReportError("M4 Go test JSONL package is outside the pinned module")
    package = package.removeprefix(GO_MODULE)
    if package not in required:
        raise ReportError("M4 Go test JSONL contains an unregistered package")
    if action == "run" and not isinstance(test_name, str):
        raise ReportError("M4 Go test run event has no test name")
    if action == "output" and test_name is not None and not isinstance(test_name, str):
        raise ReportError("M4 Go test output event has a malformed test name")
    return action, package, test_name


def _record_go_test_event(
    action: str, package: str, test_name: object, started_packages: set[str],
    observed_packages: set[str], passed: dict[str, set[str]],
) -> None:
    if action == "start":
        if test_name is not None or package in started_packages:
            raise ReportError("M4 Go package start event is malformed or duplicated")
        started_packages.add(package)
        return
    if action != "pass":
        return
    if test_name is None:
        if package in observed_packages:
            raise ReportError("M4 Go package pass appears more than once")
        observed_packages.add(package)
        return
    if not isinstance(test_name, str):
        raise ReportError("M4 Go test event has a malformed test name")
    passed.setdefault(package, set()).add(test_name)


def _validate_go_test_completion(
    required: dict[str, set[str]], started_packages: set[str],
    observed_packages: set[str], passed: dict[str, set[str]], event_count: int,
) -> None:
    if (event_count == 0 or started_packages != set(required)
            or observed_packages != set(required)):
        raise ReportError("M4 Go package completion evidence is missing")
    for package, tests in required.items():
        if not tests.issubset(passed.get(package, set())):
            raise ReportError(f"M4 required Go tests did not pass in {package}")


def _testsuite_elements(path: Path) -> list[ET.Element]:
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as error:
        raise ReportError(f"Maven XML report {path.name} is invalid") from error
    if root.tag == "testsuite":
        return [root]
    if root.tag == "testsuites":
        return list(root.iter("testsuite"))
    raise ReportError(f"Maven XML report {path.name} has an unknown root")


def _java_test_report_paths(artifact: Path) -> list[tuple[str, str, Path]]:
    reports = []
    for module in sorted(JAVA_MODULES):
        for kind in sorted(JAVA_KINDS):
            directory = artifact / module / "target" / f"{kind}-reports"
            if directory.is_dir():
                reports.extend((module, kind, path) for path in sorted(directory.glob("TEST-*.xml")))
    return reports


def _java_testcase_names(suite: ET.Element) -> set[str]:
    cases = suite.findall("testcase")
    try:
        counts = {
            field: int(suite.get(field, "-1"))
            for field in ("tests", "failures", "errors", "skipped")
        }
    except ValueError as error:
        raise ReportError("Maven report has a non-numeric counter") from error
    if (counts["tests"] < 1 or counts["tests"] != len(cases)
            or counts["failures"] or counts["errors"] or counts["skipped"]):
        raise ReportError("Maven report is empty, failed, inconsistent, or skipped")
    if any(_java_testcase_has_failure(case) for case in cases):
        raise ReportError("Maven testcase contains failure, error, or skip evidence")
    return {case.get("name", "") for case in cases}


def _java_testcase_has_failure(testcase: ET.Element) -> bool:
    return any(testcase.find(tag) is not None for tag in ("failure", "error", "skipped"))


def _read_java_test_report(path: Path, module: str, kind: str) -> dict[str, tuple[str, str, set[str]]]:
    suites = {}
    for suite in _testsuite_elements(path):
        name = suite.get("name")
        if not name or name in suites:
            raise ReportError("Maven report has an empty or duplicate suite name")
        suites[name] = (module, kind, _java_testcase_names(suite))
    return suites


def _required_java_suites(task: dict[str, object]) -> dict[str, tuple[str, str, set[str]]]:
    required: dict[str, tuple[str, str, set[str]]] = {}
    for item in task["maven_tests"]:
        class_name = item["class"]
        required.setdefault(class_name, (item["module"], item["kind"], set()))
        required[class_name][2].update(item["tests"])
    return required


def _validate_required_java_suites(
    task: dict[str, object], suites: dict[str, tuple[str, str, set[str]]],
) -> None:
    for class_name, (module, kind, required_names) in _required_java_suites(task).items():
        actual = suites.get(class_name)
        if actual is None or actual[:2] != (module, kind):
            raise ReportError(f"required Maven suite {class_name} is missing")
        if not required_names.issubset(actual[2]):
            raise ReportError(f"required Maven test cases are missing in {class_name}")


def _validate_java_tests(artifact: Path, task: dict[str, object]) -> int:
    reports = _java_test_report_paths(artifact)
    if not reports:
        raise ReportError("Maven XML evidence is missing")
    suites: dict[str, tuple[str, str, set[str]]] = {}
    for module, kind, path in reports:
        observed = _read_java_test_report(path, module, kind)
        if suites.keys() & observed.keys():
            raise ReportError("Maven report has an empty or duplicate suite name")
        suites.update(observed)
    _validate_required_java_suites(task, suites)
    return sum(len(item[2]) for item in suites.values())


def _validate_j42_runtime_metrics(artifact: Path) -> dict[str, object]:
    report_path = artifact / "server/target/failsafe-reports/TEST-io.etcdhbase.server.M4LeaseKeepAliveRpcIT.xml"
    if not report_path.is_file() or report_path.is_symlink():
        raise ReportError("J42 KeepAlive runtime metric test report is missing or unsafe")
    suites = _testsuite_elements(report_path)
    metric_lines = [
        line.removeprefix("M4_KEEPALIVE_METRICS_V1 ")
        for suite in suites
        for output in suite.iter("system-out")
        for line in (output.text or "").splitlines()
        if line.startswith("M4_KEEPALIVE_METRICS_V1 ")
    ]
    if len(metric_lines) != 2:
        raise ReportError("J42 KeepAlive metric samples do not cover both HBase layouts")

    stage_names = (
        "queue", "prepare", "publish_and_finalize", "result_resolution",
        "delivery_validation", "outbound_ready_wait",
    )
    required_fields = {
        "sampling_method", "requests", "responses", "selected", "unknown_outcomes",
        "request_bytes", "response_bytes", "auxiliary_records", "auxiliary_payload_bytes",
        "hbase_client_operations", "logical_hbase_request_payload_bytes",
    }
    for stage in stage_names:
        required_fields.update({f"{stage}_samples", f"{stage}_total_nanos"})

    totals = {field: 0 for field in required_fields if field != "sampling_method"}
    for metric_line in metric_lines:
        if len(metric_line) > 4096:
            raise ReportError("J42 KeepAlive metric sample exceeds its size limit")
        fields: dict[str, str] = {}
        for token in metric_line.split():
            name, separator, value = token.partition("=")
            if not separator or name in fields:
                raise ReportError("J42 KeepAlive metric sample is malformed")
            fields[name] = value
        if fields.get("sampling_method") != "per-runtime-cumulative-counter-snapshot":
            raise ReportError("J42 KeepAlive metric sample method is missing or changed")
        if set(fields) != required_fields:
            raise ReportError("J42 KeepAlive metric sample is missing required counters")
        for name in required_fields - {"sampling_method"}:
            raw_value = fields[name]
            if not re.fullmatch(r"[0-9]{1,20}", raw_value):
                raise ReportError("J42 KeepAlive metric sample contains a non-numeric counter")
            totals[name] += int(raw_value)
        if (int(fields["requests"]) < 8 or int(fields["responses"]) < 1
                or int(fields["selected"]) < 1 or int(fields["auxiliary_records"]) < 1
                or int(fields["auxiliary_payload_bytes"]) < 1
                or int(fields["request_bytes"]) < 1 or int(fields["response_bytes"]) < 1
                or int(fields["hbase_client_operations"]) < 1
                or int(fields["logical_hbase_request_payload_bytes"]) < 1):
            raise ReportError("J42 KeepAlive metric sample does not contain the exercised RPC and storage counters")
        if any(int(fields[f"{stage}_samples"]) < 1 for stage in stage_names):
            raise ReportError("J42 KeepAlive metric sample is missing a stage-duration population")

    queue_samples = totals["queue_samples"]
    return {
        "keepalive_sampling_method": "per-runtime-cumulative-counter-snapshot",
        "keepalive_metric_snapshots": len(metric_lines),
        "keepalive_rpc_requests": totals["requests"],
        "keepalive_rpc_responses": totals["responses"],
        "keepalive_selected_renewals": totals["selected"],
        "keepalive_unknown_outcomes": totals["unknown_outcomes"],
        "keepalive_proto_request_bytes": totals["request_bytes"],
        "keepalive_proto_response_bytes": totals["response_bytes"],
        "keepalive_auxiliary_records": totals["auxiliary_records"],
        "keepalive_auxiliary_payload_bytes": totals["auxiliary_payload_bytes"],
        "keepalive_logical_hbase_client_operations": totals["hbase_client_operations"],
        "keepalive_logical_hbase_request_payload_bytes": totals["logical_hbase_request_payload_bytes"],
        "keepalive_queue_wait_samples": queue_samples,
        "keepalive_queue_wait_total_nanos": totals["queue_total_nanos"],
        "keepalive_queue_wait_mean_nanos": totals["queue_total_nanos"] // queue_samples,
        "keepalive_stages": {
            stage: {
                "samples": totals[f"{stage}_samples"],
                "total_nanos": totals[f"{stage}_total_nanos"],
                "mean_nanos": totals[f"{stage}_total_nanos"] // totals[f"{stage}_samples"],
            }
            for stage in stage_names
        },
    }


def _task_artifact_root(artifact: Path, task: dict[str, object]) -> Path:
    subdirectory = task.get("artifact_subdir", "")
    if not isinstance(subdirectory, str):
        raise ReportError("M4 evidence subdirectory is malformed")
    if not subdirectory:
        return artifact
    relative = PurePosixPath(subdirectory)
    if (relative.is_absolute() or "\\" in subdirectory
            or any(part in {"", ".", ".."} for part in relative.parts)):
        raise ReportError("M4 evidence subdirectory is unsafe")
    current = artifact
    if current.is_symlink():
        raise ReportError("M4 evidence root must not be a symlink")
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ReportError("M4 evidence subdirectory must not traverse a symlink")
    if not current.is_dir():
        raise ReportError("M4 task evidence subdirectory is missing")
    return current


def _read_trace(
    path: Path, expected_cases: list[dict[str, object]], scope: str,
) -> tuple[bytes, list[dict[str, object]]]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise ReportError(f"required M4 trace {path.name} is missing") from error
    if not raw or len(raw) > 64 << 20 or not raw.endswith(b"\n"):
        raise ReportError(f"M4 trace {path.name} is empty, oversized, or unterminated")
    lines = raw.splitlines()
    if len(lines) != len(expected_cases):
        raise ReportError(f"M4 trace {path.name} does not contain every registered case")
    expected_ordinals: dict[str, int] = {}
    records: list[dict[str, object]] = []
    for line_number, (line, expected) in enumerate(zip(lines, expected_cases), start=1):
        try:
            record = json.loads(line, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, ReportError) as error:
            raise ReportError(f"M4 trace {path.name} line {line_number} is invalid") from error
        required_fields = {
            "schema", "case_id", "ordinal", "method", "request", "response", "error",
            "stream_terminal", "evidence_scope", "elapsed_ns",
        }
        if not isinstance(record, dict) or set(record) != required_fields:
            raise ReportError("M4 trace record envelope is incomplete or has unknown fields")
        expected_case_id = expected["id"]
        expected_case_ordinal = expected_ordinals.get(expected_case_id, 0) + 1
        expected_ordinals[expected_case_id] = expected_case_ordinal
        if (record["schema"] != TRACE_SCHEMA or record["case_id"] != expected_case_id
                or record["ordinal"] != expected_case_ordinal or record["method"] != expected["method"]
                or record["evidence_scope"] != scope):
            raise ReportError("M4 trace has a changed case, order, method, or evidence scope")
        if not isinstance(record["request"], dict):
            raise ReportError("M4 trace request snapshot is missing")
        expected_error = {
            "duplicate-id": "FailedPrecondition",
            "oversized-ttl": "OutOfRange",
            "binding-invalid-lease-atomicity": "NotFound",
            "binding-invalid-lease-put": "NotFound",
            "revoke-missing": "NotFound",
        }.get(expected_case_id)
        if expected_error is not None:
            if (not isinstance(record["error"], dict)
                    or record["error"].get("grpc_code") != expected_error
                    or record["response"] is not None):
                raise ReportError("M4 trace did not preserve its required native error")
        elif record["error"] is not None or not isinstance(record["response"], dict):
            raise ReportError("M4 successful Lease case has no response or has an error")
        expected_terminal = expected.get("stream_terminal")
        if record["method"] not in M4_STREAM_METHODS and expected_terminal is not None:
            raise ReportError("M4 unary trace registration unexpectedly has a stream terminal")
        if record["stream_terminal"] != expected_terminal:
            raise ReportError("M4 trace terminal does not match its registered stream contract")
        records.append(record)
    if [(item.get("id"), item.get("method")) for item in expected_cases] == list(J40_TRACE_CASES):
        _validate_j40_lease_trace(records)
    return raw, records


def _snapshot_fields(snapshot: object, label: str) -> dict[str, object]:
    if not isinstance(snapshot, dict) or set(snapshot) != {"fields", "oneofs"}:
        raise ReportError(f"J40 {label} snapshot is incomplete")
    fields = snapshot.get("fields")
    oneofs = snapshot.get("oneofs")
    if not isinstance(fields, dict) or not isinstance(oneofs, dict):
        raise ReportError(f"J40 {label} snapshot is malformed")
    return fields


def _trace_int64(value: object, label: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"(?:0|-?[1-9][0-9]*)", value):
        raise ReportError(f"J40 {label} is missing or is not a canonical int64")
    parsed = int(value)
    if parsed < -(1 << 63) or parsed > (1 << 63) - 1:
        raise ReportError(f"J40 {label} is outside the int64 range")
    return parsed


def _trace_uint64(value: object, label: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
        raise ReportError(f"J45 {label} is missing or is not a canonical uint64")
    parsed = int(value)
    if parsed > (1 << 64) - 1:
        raise ReportError(f"J45 {label} is outside the uint64 range")
    return parsed


def _j45_header_fields(
    response_fields: dict[str, object], label: str, *, allow_zero_raft_term: bool = False,
) -> dict[str, object]:
    header = _snapshot_fields(response_fields.get("header"), f"{label} header")
    for field in ("clusterId", "memberId"):
        if _trace_uint64(header.get(field), f"{label} header {field}") < 1:
            raise ReportError(f"J45 {label} header {field} must be positive")
    raft_term = _trace_uint64(header.get("raftTerm"), f"{label} header raftTerm")
    if raft_term < 1 and not allow_zero_raft_term:
        raise ReportError(f"J45 {label} header raftTerm must be positive")
    return header


def _j45_record_request(
    record: dict[str, object], label: str, request_kind: str,
) -> dict[str, object]:
    snapshot = record.get("request")
    request_fields = _snapshot_fields(snapshot, f"{label} request")
    oneofs = snapshot.get("oneofs") if isinstance(snapshot, dict) else None
    if oneofs != {"request_union": request_kind}:
        raise ReportError(f"J45 {label} request is not the registered raw Watch operation")
    field_name = "createRequest" if request_kind == "create_request" else "cancelRequest"
    return _snapshot_fields(request_fields.get(field_name), f"{label} {field_name}")


def _j45_watch_response(
    record: dict[str, object], label: str, *, watch_id: int, header_revision: int,
    created: bool, canceled: bool, compact_revision: int = 0, cancel_reason: str = "",
) -> None:
    response = record.get("response")
    fields = _snapshot_fields(response, f"{label} response")
    local_service = record.get("evidence_scope") == "local-service"
    header = _j45_header_fields(fields, label, allow_zero_raft_term=local_service)
    if (_trace_int64(header.get("revision"), f"{label} header revision") != header_revision
            or _trace_int64(fields.get("watchId"), f"{label} watch ID") != watch_id
            or fields.get("created") is not created or fields.get("canceled") is not canceled
            or _trace_int64(fields.get("compactRevision"), f"{label} compact revision") != compact_revision
            or fields.get("cancelReason") != cancel_reason or fields.get("events") != []):
        raise ReportError(f"J45 {label} response fields differ from the pinned etcd Watch contract")


def _validate_j45_native_history_fixture(
    cases: dict[str, dict[str, object]],
) -> tuple[dict[str, object], int]:
    history_record = cases["watch-start-history-put"]
    history_request = _snapshot_fields(history_record["request"], "J45 Watch history fixture Put request")
    history_response = _snapshot_fields(history_record["response"], "J45 Watch history fixture Put response")
    history_header = _j45_header_fields(history_response, "Watch history fixture Put")
    history_revision = _trace_int64(history_header.get("revision"), "Watch history fixture revision")
    if history_revision <= 1 or not history_request.get("key") or not history_request.get("value"):
        raise ReportError("J45 Watch history fixture Put did not establish an initial old value")
    return history_request, history_revision


def _validate_j45_native_live_probe(
    cases: dict[str, dict[str, object]], history_request: dict[str, object],
    history_revision: int,
) -> None:
    negative_watch_request = _j45_record_request(
        cases["initial-minus-one-created"], "initial-minus-one-created", "create_request"
    )
    if negative_watch_request.get("key") != history_request.get("key"):
        raise ReportError("J45 negative start did not watch the key seeded by its history fixture")

    live_created_id = "watch-negative-live-created"
    live_create_request = _j45_record_request(
        cases[live_created_id], live_created_id, "create_request"
    )
    if (_trace_int64(live_create_request.get("watchId"), "negative live Watch ID") != 45
            or _trace_int64(live_create_request.get("startRevision"), "negative live start revision") != -1
            or live_create_request.get("key") != history_request.get("key")):
        raise ReportError("J45 live negative Watch does not target its prior history fixture")
    _j45_watch_response(cases[live_created_id], live_created_id, watch_id=45,
                        header_revision=history_revision, created=True, canceled=False)

    progress_record = cases["watch-negative-live-progress"]
    progress_request = progress_record.get("request")
    progress_request_fields = _snapshot_fields(progress_request, "J45 synced Watch progress request")
    progress_oneofs = progress_request.get("oneofs") if isinstance(progress_request, dict) else None
    progress_response = _snapshot_fields(
        progress_record.get("response"), "J45 synced Watch progress response"
    )
    progress_header = _j45_header_fields(progress_response, "synced Watch progress response")
    if (progress_oneofs != {"request_union": "progress_request"}
            or not isinstance(progress_request_fields.get("progressRequest"), dict)
            or _trace_int64(progress_response.get("watchId"), "synced progress Watch ID") != -1
            or progress_response.get("created") is not False
            or progress_response.get("canceled") is not False
            or _trace_int64(progress_header.get("revision"), "synced progress revision") != history_revision
            or progress_response.get("events") != []):
        raise ReportError("J45 native negative-start event probe has no synced progress barrier")

    live_put = cases["watch-negative-live-put"]
    live_put_request = _snapshot_fields(live_put["request"], "J45 live negative Watch Put request")
    live_put_response = _snapshot_fields(live_put["response"], "J45 live negative Watch Put response")
    live_put_header = _j45_header_fields(live_put_response, "negative Watch live Put")
    live_revision = _trace_int64(live_put_header.get("revision"), "negative Watch live revision")
    if (live_put_request.get("key") != history_request.get("key")
            or not live_put_request.get("value") or live_revision != history_revision + 1):
        raise ReportError("J45 live negative Watch Put does not follow its history fixture exactly once")

    live_event_id = "watch-negative-live-event"
    event_record = cases[live_event_id]
    event_request = _j45_record_request(event_record, live_event_id, "create_request")
    event_response = _snapshot_fields(event_record["response"], f"{live_event_id} response")
    event_header = _j45_header_fields(event_response, live_event_id)
    events = event_response.get("events")
    if (event_request != live_create_request
            or _trace_int64(event_header.get("revision"), f"{live_event_id} header revision") != live_revision
            or _trace_int64(event_response.get("watchId"), f"{live_event_id} watch ID") != 45
            or event_response.get("created") is not False or event_response.get("canceled") is not False
            or not isinstance(events, list) or len(events) != 1):
        raise ReportError("J45 negative Watch did not report one post-create event at its Put revision")
    event_fields = _snapshot_fields(events[0], f"{live_event_id} event")
    event_kv = _snapshot_fields(event_fields.get("kv"), f"{live_event_id} key-value")
    # Native proto traces encode Event_EventType as its wire enum value (PUT = 0).
    if (event_fields.get("type") != 0
            or event_kv.get("key") != live_put_request.get("key")
            or event_kv.get("value") != live_put_request.get("value")
            or _trace_int64(event_kv.get("modRevision"), f"{live_event_id} mod revision") != live_revision):
        raise ReportError("J45 negative Watch delivered an old or mismatched event")

    cancel_id = "watch-negative-live-cancelled"
    cancel_request = _j45_record_request(cases[cancel_id], cancel_id, "cancel_request")
    if _trace_int64(cancel_request.get("watchId"), f"{cancel_id} Watch ID") != 45:
        raise ReportError("J45 negative Watch cleanup canceled a different Watch ID")
    _j45_watch_response(cases[cancel_id], cancel_id, watch_id=45,
                        header_revision=live_revision, created=False, canceled=True)


def _validate_j45_native_compaction_fixture(cases: dict[str, dict[str, object]]) -> int:
    put = cases["watch-compact-fixture-put"]
    put_request = _snapshot_fields(put["request"], "J45 compaction fixture Put request")
    put_response = _snapshot_fields(put["response"], "J45 compaction fixture Put response")
    put_header = _j45_header_fields(put_response, "compaction fixture Put")
    compact_revision = _trace_int64(put_header.get("revision"), "compaction fixture revision")
    if compact_revision <= 1 or not put_request.get("key") or not put_request.get("value"):
        raise ReportError("J45 compaction fixture Put did not advance a durable native revision")

    range_record = cases["watch-compact-fixture-range"]
    range_request = _snapshot_fields(range_record["request"], "J45 compaction fixture Range request")
    range_response = _snapshot_fields(range_record["response"], "J45 compaction fixture Range response")
    range_header = _j45_header_fields(range_response, "compaction fixture Range")
    kvs = range_response.get("kvs")
    if (range_request.get("key") != put_request.get("key")
            or _trace_int64(range_header.get("revision"), "compaction fixture Range revision") != compact_revision
            or not isinstance(kvs, list) or len(kvs) != 1):
        raise ReportError("J45 compaction fixture Range did not confirm the Put revision")
    fixture_kv = _snapshot_fields(kvs[0], "J45 compaction fixture key-value")
    if (fixture_kv.get("key") != put_request.get("key")
            or fixture_kv.get("value") != put_request.get("value")):
        raise ReportError("J45 compaction fixture Range returned a different key or value")

    compact_record = cases["watch-compact-fixture-compact"]
    compact_request = _snapshot_fields(compact_record["request"], "J45 compaction fixture Compact request")
    compact_response = _snapshot_fields(compact_record["response"], "J45 compaction fixture Compact response")
    compact_header = _j45_header_fields(compact_response, "compaction fixture Compact")
    if (_trace_int64(compact_request.get("revision"), "compaction request revision") != compact_revision
            or _trace_int64(compact_header.get("revision"), "compaction response revision") != compact_revision):
        raise ReportError("J45 compaction fixture did not compact its verified native revision")
    return compact_revision


def _validate_j45_native_trace(records: list[dict[str, object]]) -> None:
    cases = {record["case_id"]: record for record in records}
    history_request, history_revision = _validate_j45_native_history_fixture(cases)
    _validate_j45_native_live_probe(cases, history_request, history_revision)
    compact_revision = _validate_j45_native_compaction_fixture(cases)
    _validate_j45_watch_phase(cases, "initial", history_revision, -1)
    _validate_j45_watch_phase(cases, "compacted", compact_revision, compact_revision)


def _validate_j45_local_trace(records: list[dict[str, object]], phase: str,
                              revision: int, compact_revision: int) -> None:
    expected_case_ids = [case_id for case_id, _ in J45_TRACE_CASES
                         if case_id.startswith(f"{phase}-")]
    cases = {record["case_id"]: record for record in records}
    if list(cases) != expected_case_ids:
        raise ReportError(f"J45 local {phase} Watch trace has missing or reordered cases")
    _validate_j45_watch_phase(cases, phase, revision, compact_revision)


def _validate_j45_watch_phase(cases: dict[str, dict[str, object]], phase: str,
                              response_revision: int, compact_boundary: int) -> None:
    _validate_j45_start_revision_matrix(cases, phase, response_revision, compact_boundary)
    _validate_j45_raw_watch_id_matrix(cases, phase, response_revision)


def _validate_j45_start_revision_matrix(
    cases: dict[str, dict[str, object]], phase: str,
    response_revision: int, compact_boundary: int,
) -> None:
    for matrix_id, matrix_phase, start_revision, compacted, boundary in J45_START_MATRIX:
        if matrix_phase != phase:
            continue
        _validate_j45_start_revision_case(
            cases, matrix_id, phase, start_revision, compacted, boundary,
            response_revision, compact_boundary,
        )


def _validate_j45_start_revision_case(
    cases: dict[str, dict[str, object]], matrix_id: str, phase: str,
    start_revision: int, compacted: bool, boundary: int | str,
    response_revision: int, compact_boundary: int,
) -> None:
    _, start_name = matrix_id.split("-", 1)
    base_id = f"{phase}-{start_name}"
    watch_id = {-2: 41, -1: 42, 0: 43, 3: 44}[start_revision]
    if compacted != (start_revision != 0 and start_revision < compact_boundary):
        raise ReportError(f"J45 {phase} Watch start registration disagrees with its compact boundary")
    if ((boundary == "fixture" and phase != "compacted")
            or (boundary != "fixture" and boundary != compact_boundary)):
        raise ReportError("J45 Watch start matrix changed its compact boundary")

    created_id = f"{base_id}-created"
    created_request = _j45_record_request(cases[created_id], created_id, "create_request")
    if (_trace_int64(created_request.get("startRevision"), f"{created_id} start revision")
            != start_revision
            or _trace_int64(created_request.get("watchId"), f"{created_id} request watch ID")
            != watch_id):
        raise ReportError(f"J45 {created_id} request changed the raw Watch start revision or ID")
    _j45_watch_response(cases[created_id], created_id, watch_id=watch_id,
                        header_revision=response_revision, created=True, canceled=False)

    if compacted:
        final_cancel_id = _validate_j45_compacted_start_lifecycle(
            cases, base_id, watch_id, response_revision, compact_boundary
        )
    else:
        final_cancel_id = f"{base_id}-cancelled"
    _validate_j45_start_final_cancel(cases, final_cancel_id, watch_id, response_revision)


def _validate_j45_compacted_start_lifecycle(
    cases: dict[str, dict[str, object]], base_id: str, watch_id: int,
    response_revision: int, compact_boundary: int,
) -> str:
    compacted_id = f"{base_id}-compacted"
    compacted_request = _j45_record_request(cases[compacted_id], compacted_id, "create_request")
    created_request = _j45_record_request(cases[f"{base_id}-created"],
                                          f"{base_id}-created", "create_request")
    if compacted_request != created_request:
        raise ReportError(f"J45 {compacted_id} did not preserve the original create request")
    _j45_watch_response(cases[compacted_id], compacted_id, watch_id=watch_id,
                        header_revision=0, created=False, canceled=True,
                        compact_revision=compact_boundary)

    duplicate_id = f"{base_id}-duplicate-id"
    duplicate_request = _j45_record_request(cases[duplicate_id], duplicate_id, "create_request")
    if (_trace_int64(duplicate_request.get("startRevision"), f"{duplicate_id} start revision") != 0
            or _trace_int64(duplicate_request.get("watchId"), f"{duplicate_id} request watch ID")
            != watch_id):
        raise ReportError(f"J45 {duplicate_id} did not reuse the still-reserved Watch ID")
    _j45_watch_response(cases[duplicate_id], duplicate_id, watch_id=-1,
                        header_revision=response_revision, created=True, canceled=True,
                        cancel_reason="mvcc: duplicate watch ID provided on the WatchStream")

    cancel_id = f"{base_id}-cancel-id"
    cancel_request = _j45_record_request(cases[cancel_id], cancel_id, "cancel_request")
    if _trace_int64(cancel_request.get("watchId"), f"{cancel_id} request watch ID") != watch_id:
        raise ReportError(f"J45 {cancel_id} canceled a different Watch ID")
    _j45_watch_response(cases[cancel_id], cancel_id, watch_id=watch_id,
                        header_revision=response_revision, created=False, canceled=True)

    reused_id = f"{base_id}-reused-created"
    reused_request = _j45_record_request(cases[reused_id], reused_id, "create_request")
    if (_trace_int64(reused_request.get("startRevision"), f"{reused_id} start revision") != 0
            or _trace_int64(reused_request.get("watchId"), f"{reused_id} request watch ID") != watch_id):
        raise ReportError(f"J45 {reused_id} did not reuse the Watch ID after explicit cancellation")
    _j45_watch_response(cases[reused_id], reused_id, watch_id=watch_id,
                        header_revision=response_revision, created=True, canceled=False)
    return f"{base_id}-reused-cancelled"


def _validate_j45_start_final_cancel(
    cases: dict[str, dict[str, object]], case_id: str,
    watch_id: int, response_revision: int,
) -> None:
    cancel_request = _j45_record_request(cases[case_id], case_id, "cancel_request")
    if _trace_int64(cancel_request.get("watchId"), f"{case_id} request watch ID") != watch_id:
        raise ReportError(f"J45 {case_id} canceled a different Watch ID")
    _j45_watch_response(cases[case_id], case_id, watch_id=watch_id,
                        header_revision=response_revision, created=False, canceled=True)


def _validate_j45_raw_watch_id_matrix(
    cases: dict[str, dict[str, object]], phase: str, response_revision: int,
) -> None:

    for matrix_id, matrix_phase, watch_id, start_revision in J45_RAW_WATCH_ID_MATRIX:
        if matrix_phase != phase:
            continue
        create_id = f"{matrix_id}-created"
        create_request = _j45_record_request(cases[create_id], create_id, "create_request")
        if (_trace_int64(create_request.get("watchId"), f"{create_id} request watch ID") != watch_id
                or _trace_int64(create_request.get("startRevision"), f"{create_id} start revision")
                != start_revision):
            raise ReportError(f"J45 {create_id} changed the raw Watch ID or create checkpoint")
        _j45_watch_response(cases[create_id], create_id, watch_id=watch_id,
                            header_revision=response_revision, created=True, canceled=False)

        cancel_id = f"{matrix_id}-cancelled"
        cancel_request = _j45_record_request(cases[cancel_id], cancel_id, "cancel_request")
        if _trace_int64(cancel_request.get("watchId"), f"{cancel_id} request watch ID") != watch_id:
            raise ReportError(f"J45 {cancel_id} did not cancel the raw Watch ID")
        _j45_watch_response(cases[cancel_id], cancel_id, watch_id=watch_id,
                            header_revision=response_revision, created=False, canceled=True)


def _successful_lease_response(
    record: dict[str, object], label: str,
) -> dict[str, object]:
    if record.get("error") is not None or not isinstance(record.get("response"), dict):
        raise ReportError(f"J40 {label} did not return a successful response")
    response_fields = _snapshot_fields(record["response"], f"{label} response")
    if _lease_response_revision(response_fields, label) < 1:
        raise ReportError(f"J40 {label} has no valid response revision")
    return response_fields


def _lease_response_revision(response_fields: dict[str, object], label: str) -> int:
    header_fields = _snapshot_fields(response_fields.get("header"), f"{label} header")
    return _trace_int64(header_fields.get("revision"), f"{label} header revision")


def _validate_grant_trace(
    record: dict[str, object], label: str, *, automatic: bool,
) -> tuple[int, int]:
    request_fields = _snapshot_fields(record.get("request"), f"{label} request")
    response_fields = _successful_lease_response(record, label)
    requested_id = _trace_int64(request_fields.get("ID"), f"{label} request ID")
    requested_ttl = _trace_int64(request_fields.get("TTL"), f"{label} request TTL")
    response_id = _trace_int64(response_fields.get("ID"), f"{label} response ID")
    granted_ttl = _trace_int64(response_fields.get("TTL"), f"{label} response TTL")
    if response_fields.get("error") != "" or granted_ttl <= 0 or response_id == 0:
        raise ReportError(f"J40 {label} has an invalid granted ID or TTL")
    if automatic:
        if requested_id != 0 or requested_ttl != 0 or response_id < 1:
            raise ReportError("J40 automatic Grant request or response is invalid")
    elif requested_id == 0 or response_id != requested_id or granted_ttl != requested_ttl:
        raise ReportError(f"J40 {label} changed its explicit ID or TTL")
    return response_id, granted_ttl


def _validate_ttl_trace(
    record: dict[str, object], label: str, expected_id: int, granted_ttl: int,
) -> None:
    request_fields = _snapshot_fields(record.get("request"), f"{label} request")
    response_fields = _successful_lease_response(record, label)
    request_id = _trace_int64(request_fields.get("ID"), f"{label} request ID")
    response_id = _trace_int64(response_fields.get("ID"), f"{label} response ID")
    ttl = _trace_int64(response_fields.get("TTL"), f"{label} response TTL")
    actual_granted_ttl = _trace_int64(
        response_fields.get("grantedTTL"), f"{label} granted TTL"
    )
    keys = response_fields.get("keys")
    if (request_id != expected_id or request_fields.get("keys") is not True
            or response_id != expected_id or not 0 < ttl <= granted_ttl
            or actual_granted_ttl != granted_ttl or keys != []):
        raise ReportError(f"J40 {label} does not match the active empty Lease")


def _validate_missing_ttl_trace(
    record: dict[str, object], label: str, expected_id: int,
) -> None:
    request_fields = _snapshot_fields(record.get("request"), f"{label} request")
    response_fields = _successful_lease_response(record, label)
    if (_trace_int64(request_fields.get("ID"), f"{label} request ID") != expected_id
            or request_fields.get("keys") is not True
            or _trace_int64(response_fields.get("ID"), f"{label} response ID") != expected_id
            or _trace_int64(response_fields.get("TTL"), f"{label} response TTL") != -1
            or _trace_int64(response_fields.get("grantedTTL"), f"{label} granted TTL") != 0
            or response_fields.get("keys") != []):
        raise ReportError(f"J40 {label} does not preserve the missing-Lease response")


def _validate_lease_listing_trace(
    record: dict[str, object], label: str, expected_ids: set[int],
) -> None:
    response_fields = _successful_lease_response(record, label)
    leases = response_fields.get("leases")
    if not isinstance(leases, list):
        raise ReportError(f"J40 {label} has no Lease list")
    observed_ids = {
        _trace_int64(_snapshot_fields(item, f"{label} Lease").get("ID"), f"{label} Lease ID")
        for item in leases
    }
    if observed_ids != expected_ids or len(leases) != len(expected_ids):
        raise ReportError(f"J40 {label} is incomplete or contains duplicate Leases")


def _require_lease_error(
    record: dict[str, object], label: str, expected_code: str,
) -> dict[str, object]:
    error = record.get("error")
    if (not isinstance(error, dict) or error.get("grpc_code") != expected_code
            or record.get("response") is not None):
        raise ReportError(f"J40 {label} did not preserve {expected_code}")
    return error


def _validate_j40_successful_leases(cases: dict[str, dict[str, object]]) -> set[int]:
    baseline = _successful_lease_response(cases["native-baseline"], "native baseline")
    if baseline.get("leases") != []:
        raise ReportError("J40 native baseline is not an empty Lease list")

    automatic_id, automatic_ttl = _validate_grant_trace(
        cases["auto-grant"], "automatic Grant", automatic=True
    )
    _validate_ttl_trace(cases["auto-ttl"], "automatic TTL", automatic_id, automatic_ttl)
    explicit_id, explicit_ttl = _validate_grant_trace(
        cases["explicit-grant"], "explicit Grant", automatic=False
    )
    if explicit_id <= 0 or explicit_id == automatic_id or explicit_ttl != 30:
        raise ReportError("J40 explicit Grant does not identify its distinct 30-second Lease")
    _validate_ttl_trace(cases["explicit-ttl"], "explicit TTL", explicit_id, explicit_ttl)

    negative_id, negative_ttl = _validate_grant_trace(
        cases["negative-id"], "negative-ID Grant", automatic=False
    )
    negative_request = _snapshot_fields(cases["negative-id"].get("request"), "negative-ID request")
    if (negative_id >= 0 or negative_ttl != 30
            or _trace_int64(negative_request.get("TTL"), "negative-ID request TTL") != 30):
        raise ReportError("J40 negative-ID Grant did not preserve the signed ID and TTL")
    _validate_lease_listing_trace(
        cases["leases-list"], "Lease listing", {automatic_id, explicit_id, negative_id}
    )
    _validate_j40_revision_stability(cases)
    return {automatic_id, explicit_id, negative_id}


def _validate_j40_failed_and_missing_leases(
    cases: dict[str, dict[str, object]], allocated_ids: set[int],
) -> None:
    duplicate_request = _snapshot_fields(cases["duplicate-id"].get("request"), "duplicate ID request")
    duplicate_id = _trace_int64(duplicate_request.get("ID"), "duplicate ID request ID")
    explicit_response = _successful_lease_response(cases["explicit-grant"], "explicit Grant")
    explicit_id = _trace_int64(explicit_response.get("ID"), "explicit Grant response ID")
    if duplicate_id != explicit_id or duplicate_id not in allocated_ids:
        raise ReportError("J40 duplicate Grant did not reuse the explicit ID")
    _require_lease_error(cases["duplicate-id"], "duplicate Grant", "FailedPrecondition")

    missing_request = _snapshot_fields(cases["missing-ttl"].get("request"), "missing TTL request")
    missing_id = _trace_int64(missing_request.get("ID"), "missing TTL request ID")
    if missing_id <= 0 or missing_id in allocated_ids:
        raise ReportError("J40 missing TTL case does not use an unallocated ID")
    _validate_missing_ttl_trace(cases["missing-ttl"], "missing TTL", missing_id)

    oversized_request = _snapshot_fields(cases["oversized-ttl"].get("request"), "oversized TTL request")
    if (_trace_int64(oversized_request.get("ID"), "oversized TTL request ID") != 0
            or _trace_int64(oversized_request.get("TTL"), "oversized TTL request TTL") != 9_000_000_001):
        raise ReportError("J40 oversized TTL case did not use the registered boundary")
    _require_lease_error(cases["oversized-ttl"], "oversized TTL Grant", "OutOfRange")

    zero_request = _snapshot_fields(cases["zero-id-ttl"].get("request"), "zero-ID TTL request")
    if _trace_int64(zero_request.get("ID"), "zero-ID TTL request ID") != 0:
        raise ReportError("J40 zero-ID TTL case changed its request")
    _validate_missing_ttl_trace(cases["zero-id-ttl"], "zero-ID TTL", 0)


def _validate_j40_revision_stability(cases: dict[str, dict[str, object]]) -> None:
    baseline = _successful_lease_response(cases["native-baseline"], "native baseline")
    baseline_revision = _lease_response_revision(baseline, "native baseline")
    successful_cases = (
        "auto-grant", "auto-ttl", "explicit-grant", "explicit-ttl", "missing-ttl",
        "negative-id", "leases-list", "zero-id-ttl",
    )
    for case_id in successful_cases:
        response = _successful_lease_response(cases[case_id], case_id)
        if _lease_response_revision(response, case_id) != baseline_revision:
            raise ReportError("J40 Lease management RPC changed the KV revision")


def _validate_j40_lease_trace(records: list[dict[str, object]]) -> None:
    if len(records) != len(J40_TRACE_CASES):
        raise ReportError("J40 trace does not contain the complete Lease scenario")
    cases = {record["case_id"]: record for record in records}
    allocated_ids = _validate_j40_successful_leases(cases)
    _validate_j40_failed_and_missing_leases(cases, allocated_ids)


def _validate_run_metadata(
    path: Path, *, run: dict[str, object], descriptor: dict[str, object], trace_bytes: bytes,
    suite_hash: str, prefix_hash: str, layout: str,
) -> None:
    metadata = read_json(path)
    if not isinstance(metadata, dict):
        raise ReportError("M4 driver metadata is malformed")
    if (metadata.get("schema") != RUN_SCHEMA or metadata.get("status") != "passed"
            or metadata.get("run_id") != run["run_id"]
            or metadata.get("source_commit") != run["source_commit"]
            or metadata.get("suite_sha256") != suite_hash
            or metadata.get("evidence_scope") != descriptor["scope"]
            or metadata.get("prefix_sha256") != prefix_hash
            or metadata.get("trace_sha256") != hashlib.sha256(trace_bytes).hexdigest()
            or metadata.get("embedded_native") is not descriptor["embedded_native"]
            or metadata.get("layout") != layout):
        raise ReportError(f"M4 driver metadata {path.name} does not match this run")


def _load_run_and_task(
    artifact: Path, registry: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    run = read_json(artifact / "run.json")
    if not isinstance(run, dict):
        raise ReportError("M4 run manifest is malformed")
    tasks = run.get("tasks")
    if (run.get("schema") != RUN_SCHEMA or run.get("status") != "passed"
            or not isinstance(tasks, list) or len(tasks) != 1):
        raise ReportError("M4 run manifest is incomplete or did not pass")
    task_id = tasks[0]
    task = next((item for item in registry["tasks"] if item["id"] == task_id), None)
    if task is None:
        raise ReportError("M4 run task is not registered")
    expected_scope = "public-local-unit" if task_id == "J45" else "public-local-minicluster"
    if run.get("scope") != expected_scope:
        raise ReportError("M4 run scope does not match its task verification profile")
    _validate_run_identity(artifact, run)
    return run, task


def _validate_run_identity(artifact: Path, run: dict[str, object]) -> None:
    run_id = run.get("run_id")
    source_commit = run.get("source_commit")
    if (not isinstance(run_id, str) or run_id != artifact.name
            or not isinstance(source_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", source_commit)
            or run.get("checkout") != source_commit
            or not re.fullmatch(r"[0-9a-f]{40}", str(run.get("base_commit", "")))
            or not isinstance(run.get("worktree_clean"), bool)
            or not isinstance(run.get("started_at"), str)
            or not isinstance(run.get("finished_at"), str)):
        raise ReportError("M4 run identity or source binding is invalid")


def _scenario_hashes(
    repository: Path, task: dict[str, object], run_id: str,
) -> tuple[str, str]:
    manifest_name = task.get("manifest")
    if not isinstance(manifest_name, str) or Path(manifest_name).name != manifest_name:
        raise ReportError("registered M4 scenario manifest path is unsafe")
    suite_path = repository / "testkit/scenarios" / manifest_name
    try:
        suite_hash = hashlib.sha256(suite_path.read_bytes()).hexdigest()
    except OSError as error:
        raise ReportError("registered M4 scenario manifest is missing") from error
    prefix = f"etcd-hbase-m4-test/{run_id}/"
    if task.get("id") == "J41":
        prefix += "bindings/"
    elif task.get("id") == "J42":
        prefix += "keepalive/"
    elif task.get("id") == "J43":
        prefix += "revoke/"
    elif task.get("id") == "J45":
        prefix += "watch/"
    prefix_hash = hashlib.sha256(prefix.encode()).hexdigest()
    return suite_hash, prefix_hash


def _validate_native_evidence(
    evidence_root: Path, run: dict[str, object], task: dict[str, object],
    suite_hash: str, prefix_hash: str,
) -> None:
    expected_cases = task["trace_cases"]
    descriptor = task["evidence"]["native"]
    trace_bytes, _ = _read_trace(
        evidence_root / "native-trace.jsonl", expected_cases, descriptor["scope"]
    )
    _validate_run_metadata(
        evidence_root / "native-metadata.json", run=run, descriptor=descriptor,
        trace_bytes=trace_bytes, suite_hash=suite_hash, prefix_hash=prefix_hash, layout="",
    )
    log_path = evidence_root / "native.log"
    marker = "M4 Watch start scenario passed" if task.get("id") == "J45" else "M4 Lease scenario passed"
    if not log_path.is_file() or marker not in log_path.read_text(encoding="utf-8"):
        raise ReportError("native Go M4 run has no success record")


def _validate_target_evidence(
    evidence_root: Path, run: dict[str, object], task: dict[str, object],
    suite_hash: str, prefix_hash: str,
) -> list[str]:
    expected_cases = task["trace_cases"]
    layouts: list[str] = []
    for descriptor in task["evidence"]["targets"]:
        layout = descriptor["layout"]
        suffix = layout.lower()
        trace_bytes, _ = _read_trace(
            evidence_root / f"target-{suffix}-trace.jsonl", expected_cases, descriptor["scope"]
        )
        _validate_run_metadata(
            evidence_root / f"target-{suffix}-metadata.json", run=run, descriptor=descriptor,
            trace_bytes=trace_bytes, suite_hash=suite_hash, prefix_hash=prefix_hash, layout=layout,
        )
        log_path = evidence_root / f"target-{suffix}.log"
        if not log_path.is_file() or "M4 Lease scenario passed" not in log_path.read_text(encoding="utf-8"):
            raise ReportError(f"target Go Lease run for {layout} has no success record")
        layouts.append(layout)
    return layouts

def _validate_comparison_evidence(evidence_root: Path, task: dict[str, object]) -> None:
    success_marker = {
        "J41": J41_COMPARE_SUCCESS,
        "J42": J42_COMPARE_SUCCESS,
        "J43": J43_COMPARE_SUCCESS,
    }.get(task.get("id"), COMPARE_SUCCESS)
    for compare_log in task["evidence"]["compare_logs"]:
        path = evidence_root / compare_log
        if not path.is_file() or success_marker not in path.read_text(encoding="utf-8"):
            raise ReportError(f"M4 native comparison {compare_log} is missing or failed")


def _validate_j29_4_evidence(repository: Path, evidence_root: Path,
                             run: dict[str, object]) -> dict[str, object]:
    path = evidence_root / "mixed-lease-load.json"
    report = read_json(path)
    if not isinstance(report, dict):
        raise ReportError("J29.4 mixed Lease load evidence is missing or malformed")
    if (report.get("schema") != "etcd-hbase-m4-j29.4/v1"
            or report.get("status") != "passed"
            or report.get("run_id") != run["run_id"]
            or report.get("source_commit") != run["source_commit"]
            or report.get("scope") != "public-local-minicluster"
            or report.get("protocol") != PROTOCOL
            or report.get("target_capacity_accepted") is not False
            or report.get("physical_hbase_rpc_bytes_collected") is not False
            or report.get("recovery_reserve_usage_measured") is not False
            or report.get("listener_max_concurrent_requests") != 8
            or report.get("listener_max_queued_requests") != 0
            or report.get("latency_sampling") != "successful-client-rpcs"):
        raise ReportError("J29.4 evidence does not match the M4 run identity or scope")

    source_files = report.get("source_files")
    if not isinstance(source_files, dict) or set(source_files) != set(J29_4_SOURCE_FILES):
        raise ReportError("J29.4 source file manifest is incomplete")
    source_digest = hashlib.sha256()
    for relative in J29_4_SOURCE_FILES:
        recorded_hash = source_files.get(relative)
        source_path = repository / relative
        if (not isinstance(recorded_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", recorded_hash)
                or not source_path.is_file() or source_path.is_symlink()):
            raise ReportError("J29.4 source file hash is malformed or references an unsafe path")
        actual_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
        if recorded_hash != actual_hash:
            raise ReportError(f"J29.4 source file changed after measurement: {relative}")
        source_digest.update(relative.encode("utf-8"))
        source_digest.update(b"\0")
        source_digest.update(recorded_hash.encode("ascii"))
        source_digest.update(b"\0")
    if report.get("source_sha256") != source_digest.hexdigest():
        raise ReportError("J29.4 aggregate source SHA-256 does not match its file manifest")

    reserve = report.get("recovery_reserve")
    reserve_fields = (
        "requests", "queued", "operations", "scanners", "cleanup", "queued_bytes",
        "memory_bytes", "expiry_worker_memory_bytes", "expiry_worker_read_physical_rows",
        "expiry_worker_read_cell_bytes",
    )
    if (not isinstance(reserve, dict) or reserve.get("scope") != "configured-per-table-group"
            or reserve.get("runtime_usage_peak_measured") is not False):
        raise ReportError("J29.4 recovery reserve is missing or overstates runtime measurements")
    for field in reserve_fields:
        minimum = 0 if field in {"queued", "queued_bytes"} else 1
        value = reserve.get(field)
        if type(value) is not int or value < minimum:
            raise ReportError("J29.4 recovery reserve values are incomplete or invalid")

    layouts = report.get("layouts")
    if not isinstance(layouts, list) or len(layouts) != 2:
        raise ReportError("J29.4 must include exactly two HBase layout runs")
    observed_layouts: set[str] = set()
    for layout_report in layouts:
        if not isinstance(layout_report, dict):
            raise ReportError("J29.4 layout evidence is malformed")
        layout = layout_report.get("layout")
        rows = layout_report.get("concurrency")
        if layout not in {"HISTORY", "NATIVE"} or layout in observed_layouts:
            raise ReportError("J29.4 layout is missing, duplicated, or unsupported")
        observed_layouts.add(layout)
        if not isinstance(rows, list) or len(rows) != 3:
            raise ReportError("J29.4 concurrency evidence must contain three rows per layout")
        if [row.get("concurrency") if isinstance(row, dict) else None for row in rows] != [1, 2, 4]:
            raise ReportError("J29.4 concurrency levels must be 1, 2, and 4 in order")
        for row in rows:
            if row.get("layout") != layout:
                raise ReportError("J29.4 row layout differs from its parent evidence")
            for field in (
                "concurrency", "iterations_per_worker", "elapsed_nanos", "keepalive_requests",
                "keepalive_client_attempts", "keepalive_client_resource_exhausted",
                "keepalive_responses", "selected_renewals", "business_puts",
                "business_put_attempts", "business_put_resource_exhausted", "combined_operations",
                "keepalive_client_rpc_p99_samples", "owner_queue_wait_samples",
                "owner_queue_wait_total_nanos", "owner_queue_wait_mean_nanos",
                "renewal_logical_hbase_client_operations", "renewal_logical_hbase_request_payload_bytes",
                "unknown_outcomes", "request_proto_bytes", "response_proto_bytes",
            ):
                value = row.get(field)
                minimum = 1 if field in {
                    "concurrency", "elapsed_nanos", "keepalive_requests", "keepalive_client_attempts",
                    "keepalive_responses", "business_put_attempts",
                    "selected_renewals", "business_puts", "combined_operations",
                    "keepalive_client_rpc_p99_samples", "owner_queue_wait_samples",
                    "renewal_logical_hbase_client_operations", "renewal_logical_hbase_request_payload_bytes",
                    "request_proto_bytes", "response_proto_bytes",
                } else 0
                if type(value) is not int or value < minimum:
                    raise ReportError(f"J29.4 metric {field} is missing or invalid")
            requests = row["keepalive_requests"]
            responses = row["keepalive_responses"]
            puts = row["business_puts"]
            attempts = row["keepalive_client_attempts"]
            queue_samples = row["owner_queue_wait_samples"]
            if (attempts != responses + row["keepalive_client_resource_exhausted"]
                    or row["business_put_attempts"] != puts + row["business_put_resource_exhausted"]
                    or row["business_put_attempts"] != attempts or requests > attempts
                    or not responses <= row["selected_renewals"] <= requests
                    or queue_samples > requests or row["unknown_outcomes"] > requests
                    or row["keepalive_client_rpc_p99_samples"] != responses
                    or row["combined_operations"] != responses + puts):
                raise ReportError("J29.4 workload counts contradict the observed RPCs")
            if row["owner_queue_wait_mean_nanos"] != row["owner_queue_wait_total_nanos"] // queue_samples:
                raise ReportError("J29.4 owner queue mean contradicts its sample count and total")
            if row["run_mode"] == "fixed-iterations":
                if row["concurrency"] == 4 or row["iterations_per_worker"] != 24:
                    raise ReportError("J29.4 fixed workload profile changed")
                if attempts != row["concurrency"] * row["iterations_per_worker"]:
                    raise ReportError("J29.4 fixed workload did not complete all worker iterations")
            elif row["run_mode"] == "until-expiry-revocation":
                if row["concurrency"] != 4 or row["iterations_per_worker"] != 0:
                    raise ReportError("J29.4 expiry overlap profile changed")
                _validate_j29_4_expiry(row.get("expiry"))
            else:
                raise ReportError("J29.4 workload run mode is unsupported")
            _validate_j29_4_latency(row.get("keepalive_client_rpc_latency_nanos"), responses)
            _validate_j29_4_latency(row.get("business_put_client_rpc_latency_nanos"), puts)
            for field, expected in (
                ("logical_hbase_operations_per_server_keepalive_request",
                 row["renewal_logical_hbase_client_operations"] / requests),
                ("logical_hbase_request_payload_bytes_per_server_keepalive_request",
                 row["renewal_logical_hbase_request_payload_bytes"] / requests),
            ):
                value = row.get(field)
                if (isinstance(value, bool) or not isinstance(value, (int, float))
                        or not math.isfinite(value) or not math.isclose(value, expected, rel_tol=1e-12)):
                    raise ReportError(f"J29.4 metric {field} contradicts the recorded totals")

    if observed_layouts != {"HISTORY", "NATIVE"}:
        raise ReportError("J29.4 does not cover both HBase layouts")

    takeover = report.get("owner_takeover")
    if (not isinstance(takeover, dict)
            or takeover.get("scenario") != "two-runtime-mtls-owner-takeover"
            or takeover.get("new_owner") != "owner-b"):
        raise ReportError("J29.4 two-runtime owner takeover evidence is missing")
    route = base64.urlsafe_b64encode(hashlib.sha256(b"owner-b").digest()).decode().rstrip("=")
    instance = takeover.get("new_owner_instance")
    prefix = f"~{route}:"
    if not isinstance(instance, str) or not instance.startswith(prefix):
        raise ReportError("J29.4 takeover owner incarnation does not match owner-b")
    try:
        incarnation = instance[len(prefix):]
        if str(uuid.UUID(incarnation)) != incarnation:
            raise ValueError("noncanonical UUID")
    except ValueError as error:
        raise ReportError("J29.4 takeover owner incarnation is invalid") from error
    old_epoch = takeover.get("old_owner_epoch")
    new_epoch = takeover.get("new_owner_epoch")
    elapsed_millis = takeover.get("elapsed_millis")
    if (type(old_epoch) is not int or old_epoch < 1 or type(new_epoch) is not int
            or new_epoch <= old_epoch or type(elapsed_millis) is not int or elapsed_millis < 0):
        raise ReportError("J29.4 owner takeover epoch or elapsed time is invalid")
    return {"layouts": sorted(observed_layouts), "concurrency": [1, 2, 4],
            "owner_takeover_millis": elapsed_millis}


def _validate_j29_4_latency(value: object, expected_samples: int) -> None:
    if not isinstance(value, dict) or value.get("samples") != expected_samples:
        raise ReportError("J29.4 RPC latency sample count is missing or inconsistent")
    samples = [value.get(field) for field in ("p50_nanos", "p95_nanos", "p99_nanos")]
    if any(type(sample) is not int or sample < 0 for sample in samples):
        raise ReportError("J29.4 RPC latency percentile is invalid")
    if not samples[0] <= samples[1] <= samples[2]:
        raise ReportError("J29.4 RPC latency percentiles are out of order")


def _validate_j29_4_expiry(value: object) -> None:
    fields = (
        "active_workers_at_revoke", "revocations", "stale_candidates", "candidates",
        "scan_rows", "scan_bytes", "retries", "trusted_delay_samples",
        "missing_trusted_delay_samples", "trusted_delay_total_millis",
        "observed_deadline_to_revoke_millis", "owner_epoch", "checkpoint_sequence",
        "window_stop_exclusive",
    )
    if not isinstance(value, dict) or value.get("expired_during_mixed_load") is not True:
        raise ReportError("J29.4 expiry was not observed during the mixed workload")
    for field in fields:
        raw = value.get(field)
        minimum = 1 if field in {
            "active_workers_at_revoke", "revocations", "stale_candidates", "candidates",
            "scan_rows", "owner_epoch", "checkpoint_sequence",
        } else 0
        if type(raw) is not int or raw < minimum:
            raise ReportError(f"J29.4 expiry metric {field} is missing or invalid")
    if value["trusted_delay_samples"] + value["missing_trusted_delay_samples"] != value["revocations"]:
        raise ReportError("J29.4 expiry delay sample accounting is inconsistent")


def _validate_j45_native_artifact(
    run: dict[str, object],
    task: dict[str, object], evidence_root: Path,
    suite_hash: str, prefix_hash: str,
) -> int:
    descriptor = task["evidence"]["native"]
    trace_bytes, records = _read_trace(
        evidence_root / "native-trace.jsonl", task["trace_cases"], descriptor["scope"]
    )
    _validate_j45_native_trace(records)
    _validate_run_metadata(
        evidence_root / "native-metadata.json", run=run, descriptor=descriptor,
        trace_bytes=trace_bytes, suite_hash=suite_hash, prefix_hash=prefix_hash, layout="",
    )
    native_log = evidence_root / "native.log"
    if (not native_log.is_file()
            or "M4 Watch start scenario passed" not in native_log.read_text(encoding="utf-8")):
        raise ReportError("J45 native raw-proto Watch oracle has no success record")
    return len(records)


def _validate_j45_local_phase_artifact(
    evidence_root: Path, phase: str, revision: int, compact_revision: int,
    local_descriptor: dict[str, object], run: dict[str, object], suite_hash: str,
) -> int:
    expected_cases = [
        {"id": case_id, "method": method}
        for case_id, method in J45_TRACE_CASES
        if case_id.startswith(f"{phase}-")
    ]
    local_root = evidence_root / "local-service"
    trace_path = local_root / f"{phase}-trace.jsonl"
    trace_bytes, records = _read_trace(trace_path, expected_cases, local_descriptor["scope"])
    _validate_j45_local_trace(records, phase, revision, compact_revision)

    run_id = run["run_id"]
    prefix = f"etcd-hbase-m4-test/{run_id}/watch/{phase}/"
    prefix_hash = hashlib.sha256(prefix.encode()).hexdigest()
    metadata_path = local_root / f"{phase}-metadata.json"
    _validate_run_metadata(
        metadata_path, run=run, descriptor=local_descriptor,
        trace_bytes=trace_bytes, suite_hash=suite_hash,
        prefix_hash=prefix_hash, layout="",
    )
    metadata = read_json(metadata_path)
    if (metadata.get("watch_phase") != phase
            or type(metadata.get("watch_revision")) is not int
            or metadata.get("watch_revision") != revision
            or type(metadata.get("watch_compact_revision")) is not int
            or metadata.get("watch_compact_revision") != compact_revision):
        raise ReportError(f"J45 local {phase} Watch metadata differs from its fixture")

    log_path = local_root / f"{phase}.log"
    log_text = log_path.read_text(encoding="utf-8") if log_path.is_file() else ""
    if "M4 Watch start scenario passed" not in log_text:
        raise ReportError(f"J45 local {phase} Watch fixture has no success record")
    return len(records)


def _validate_j45_local_lifecycle_artifact(evidence_root: Path) -> None:
    lifecycle_log = evidence_root / "local-lifecycle/initial.log"
    log_text = lifecycle_log.read_text(encoding="utf-8") if lifecycle_log.is_file() else ""
    if "M4 Watch lifecycle interop passed" not in log_text:
        raise ReportError("J45 local initial Watch fixture has no lifecycle success record")


def _validate_j45_artifact(
    repository: Path, artifact: Path, run: dict[str, object],
    task: dict[str, object], java_test_count: int,
) -> dict[str, object]:
    if any(kind != "surefire" for _, kind, _ in _java_test_report_paths(artifact)):
        raise ReportError("J45 evidence must contain JVM unit reports only")
    go_test_count = _validate_go_tests(artifact, task)
    evidence_root = _task_artifact_root(artifact, task)
    suite_hash, prefix_hash = _scenario_hashes(repository, task, run["run_id"])
    native_trace_cases = _validate_j45_native_artifact(
        run, task, evidence_root, suite_hash, prefix_hash
    )

    local_descriptor = task["evidence"]["local_service"]
    local_trace_cases = sum(
        _validate_j45_local_phase_artifact(
            evidence_root, phase, revision, compact_revision,
            local_descriptor, run, suite_hash,
        )
        for phase, revision, compact_revision in J45_LOCAL_SERVICE_PHASES
    )
    _validate_j45_local_lifecycle_artifact(evidence_root)
    return {
        "task": run["tasks"][0],
        "run_id": run["run_id"],
        "source_commit": run["source_commit"],
        "base_commit": run["base_commit"],
        "evidence_class": "official-clean" if run["worktree_clean"] else "development-dirty",
        "go_test_count": go_test_count,
        "java_test_count": java_test_count,
        "native_trace_cases": native_trace_cases,
        "native_watch_start_cases": len(J45_START_MATRIX),
        "native_watch_id_cases": len(J45_RAW_WATCH_ID_MATRIX),
        "local_service_trace_cases": local_trace_cases,
        "evidence": "JVM-Watch-units-plus-native-start-and-local-service-start-and-lifecycle-matrices",
        "scope": "public-local-unit",
    }


def validate_artifact(repository: Path, artifact: Path, registry: dict[str, object]) -> dict[str, object]:
    run, task = _load_run_and_task(artifact, registry)
    _validate_commands(artifact, run, task)
    java_test_count = _validate_java_tests(artifact, task)
    if task.get("id") == "J44":
        return {
            "task": run["tasks"][0],
            "run_id": run["run_id"],
            "source_commit": run["source_commit"],
            "base_commit": run["base_commit"],
            "evidence_class": "official-clean" if run["worktree_clean"] else "development-dirty",
            "go_test_count": 0,
            "java_test_count": java_test_count,
            "evidence": "real-HBase-MiniCluster-event-and-layout-tests",
        }
    if task.get("id") == "J45":
        return _validate_j45_artifact(repository, artifact, run, task, java_test_count)
    if task.get("id") == "J46":
        report_kinds: set[str] = set()
        report_classes: set[str] = set()
        for module, kind, path in _java_test_report_paths(artifact):
            report_kinds.add(kind)
            for suite in _testsuite_elements(path):
                class_name = suite.get("name")
                if class_name:
                    report_classes.add(class_name)
        if (report_kinds != {"surefire", "failsafe"}
                or report_classes != set(J46_JAVA_CLASS_MODULES)):
            raise ReportError(
                "J46 evidence must contain only the registered unit and MiniCluster integration reports"
            )
        go_test_count = _validate_go_tests(artifact, task)
        return {
            "task": run["tasks"][0],
            "run_id": run["run_id"],
            "source_commit": run["source_commit"],
            "base_commit": run["base_commit"],
            "evidence_class": "official-clean" if run["worktree_clean"] else "development-dirty",
            "go_test_count": go_test_count,
            "java_test_count": java_test_count,
            "evidence": "J46.1-replay-J46.2-progress-J46.3-fragment-Go-client-MiniCluster-HISTORY-NATIVE",
            "scope": "public-local-minicluster",
        }
    if task.get("id") == "J47":
        report_kinds: set[str] = set()
        report_classes: set[str] = set()
        for _, kind, path in _java_test_report_paths(artifact):
            report_kinds.add(kind)
            for suite in _testsuite_elements(path):
                class_name = suite.get("name")
                if class_name:
                    report_classes.add(class_name)
        if (report_kinds != {"surefire", "failsafe"}
                or report_classes != set(J47_JAVA_CLASS_MODULES)):
            raise ReportError(
                "J47 evidence must contain only the registered unit and MiniCluster lifecycle reports"
            )
        go_test_count = _validate_go_tests(artifact, task)
        return {
            "task": run["tasks"][0],
            "run_id": run["run_id"],
            "source_commit": run["source_commit"],
            "base_commit": run["base_commit"],
            "evidence_class": "official-clean" if run["worktree_clean"] else "development-dirty",
            "go_test_count": go_test_count,
            "java_test_count": java_test_count,
            "evidence": "J47-flow-control-compaction-cleanup-MiniCluster-HISTORY-NATIVE",
            "scope": "public-local-minicluster",
        }
    evidence_root = _task_artifact_root(artifact, task)
    go_test_count = _validate_go_tests(artifact, task)
    run_id = run["run_id"]
    source_commit = run["source_commit"]
    suite_hash, prefix_hash = _scenario_hashes(repository, task, run_id)
    _validate_native_evidence(evidence_root, run, task, suite_hash, prefix_hash)
    layouts = _validate_target_evidence(evidence_root, run, task, suite_hash, prefix_hash)
    _validate_comparison_evidence(evidence_root, task)
    j29_4 = _validate_j29_4_evidence(repository, evidence_root, run) if task["id"] == "J43" else None
    summary = {
        "task": run["tasks"][0],
        "run_id": run_id,
        "source_commit": source_commit,
        "base_commit": run["base_commit"],
        "evidence_class": "official-clean" if run["worktree_clean"] else "development-dirty",
        "go_test_count": go_test_count,
        "java_test_count": java_test_count,
        "native_trace_cases": len(task["trace_cases"]),
        "target_layouts": sorted(layouts),
    }
    if task["id"] == "J42":
        summary.update(_validate_j42_runtime_metrics(artifact))
    if j29_4 is not None:
        summary["j29_4_layouts"] = j29_4["layouts"]
        summary["j29_4_concurrency"] = j29_4["concurrency"]
        summary["owner_takeover_millis"] = j29_4["owner_takeover_millis"]
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-registry", action="store_true")
    group.add_argument("--validate-all", action="store_true")
    group.add_argument("--artifact-root")
    arguments = parser.parse_args(argv)
    try:
        registry = load_registry()
        if arguments.validate_registry:
            print("M4 evidence registry is valid")
            return 0
        if arguments.validate_all:
            validate_all_tasks_registered(registry)
            print("All M4 task registrations are available")
            return 0
        artifact = artifact_location(REPO, arguments.artifact_root)
        summary = validate_artifact(REPO, artifact, registry)
        run = read_json(artifact / "run.json")
        if not isinstance(run, dict):
            raise ReportError("M4 run manifest is malformed")
        validate_repository_binding(REPO, run)
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return 0
    except ReportError as error:
        print(f"M4 evidence rejected: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
