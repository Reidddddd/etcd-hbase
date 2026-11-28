#!/usr/bin/env python3
"""Strict M5 task registry and current-run Java test evidence validator."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
import xml.etree.ElementTree as ET


REPO = Path(__file__).resolve().parents[2]
REGISTRY = REPO / "testkit/cases/m5"
KNOWN_TASKS = {f"J{number}" for number in range(50, 60)}
TASK_SCHEMA = "etcd-hbase-m5-task/v1"
RUN_SCHEMA = "etcd-hbase-m5-run/v1"
PROTOCOL = "etcd-v3.5.23"
TASK_KEYS = {
    "schema", "task", "status", "scope", "execution_profile", "maven_reactor",
    "maven_profiles", "backend_adapters", "layouts", "scenarios", "tests",
}
TEST_KEYS = {"module", "kind", "class", "methods"}
COMMANDS = {"registry-check", "report-tests", "maven-verify", "report-verify"}
MODULES = {"protocol", "spi", "core", "hbase", "grpc", "admin", "server"}
TEST_KINDS = {"surefire", "failsafe"}
J50_SCENARIOS = {
    "opaque-token-and-encrypted-credential-envelope",
    "actual-user-authentication-and-permission-check",
    "two-user-identity-isolation",
    "independent-process-session-resolution",
    "full-process-stop-and-token-reactivation",
    "persisted-delegation-token-session-across-process-restart",
    "persisted-password-sasl-session-across-process-restart",
    "scope-mismatch-rejected",
    "ordinary-user-denied-auth-control-read",
    "authenticate-rpc-to-non-owner-kv-txn-owner-revalidation",
    "authenticate-on-a-b-real-rpc-kv-owner-forwarding-full-stop-restart",
    "authenticated-delete-range-through-non-owner-to-current-owner",
    "provider-capacity-retained-until-physical-producer-drain",
    "missing-or-zero-test-report-fails",
}
J50_REQUIRED_TESTS = {
    ("hbase", "failsafe", "io.etcdhbase.hbase.storage.HBaseAuthControlIT"):
        {"provisionsExactDescriptorAndPersistsSessionAuthorityAndSlotCas"},
    ("server", "failsafe", "io.etcdhbase.server.M5AuthProcessIT"):
        {"sessionsActivateAllRegisteredHBaseMechanismsAcrossProcessesAndFullRestart"},
    ("server", "failsafe", "io.etcdhbase.server.M5AuthenticateGatewayProcessIT"):
        {"authenticatesOnAThenUsesBGetPutTxnAndReactivatesAfterFullStop"},
    ("server", "surefire", "io.etcdhbase.server.auth.AuthSessionServiceTest"):
        {"authenticatesPersistsOnlyDigestAndEnvelopeAndResolvesOnAnotherReplica",
         "undeliveredTokenCleanupRetiresOnlyItsSessionAndSlotGeneration",
         "undeliveredCleanupRetainsTheSlotWhenTheSessionIdentityIsUncertain",
         "failedRestorableVerificationDiscardsIdentityAndReleasesSlot",
         "synchronousRestorableFailureDiscardsIdentityAndReleasesSlot",
         "cancelledRpcReleasesReservationAfterRealProducerDrains",
         "cancelledAuthenticationKeepsItsSlotUntilPhysicalProducersDrain",
         "unknownSessionWriteOutcomeKeepsTheGlobalSlotReserved",
         "revokedAuthorityRejectsAnOtherwiseActiveToken"},
    ("server", "surefire", "io.etcdhbase.server.auth.ProviderSessionIdentityManagerTest"):
        {"concurrentBorrowsShareColdActivationAndRetireOnlyAfterTheLastRelease",
         "cancellingOneBorrowDoesNotCancelTheSharedColdActivation",
         "lateActivationWithoutBorrowersIsClosedBeforeCapacityIsReused"},
    ("server", "surefire", "io.etcdhbase.server.commit.AuthenticatedRequestIdentityDrainTest"):
        {"cancelledRequestRetainsProviderCapacityUntilExceptionalPhysicalDrain"},
    ("server", "surefire", "io.etcdhbase.server.auth.AuthenticateRpcHandlerTest"):
        {"authenticatesAndReturnsOnlyTheIssuedOpaqueToken",
         "mapsCredentialRejectionWithoutReturningATokenOrSecret",
         "cancellationBeforeAuthenticateTokenSendRetiresTheUndeliveredSession"},
    ("server", "surefire", "io.etcdhbase.server.commit.SessionAuthorizationResolverTest"):
        {"ownerRejectsReferenceWithReusedSlotGenerationAndReleasesResolvedIdentity",
         "ownerRejectsIdentityFromAnotherTableGroupAndReleasesIt",
         "unauthenticatedPeerCannotResolveSession"},
    ("server", "surefire", "io.etcdhbase.server.commit.AuthenticatedRequestIdentityResolverTest"):
        {"missingTokenIsRejectedWhenAuthenticationIsEnabled",
         "unauthenticatedModeUsesServiceIdentityForOrdinaryProvider",
         "suppliedTokenWithoutSessionServiceRemainsUnavailable"},
    ("server", "surefire", "io.etcdhbase.server.commit.TxnGatewayAuthenticationTest"):
        {"rangeUsesResolvedSessionIdentityAndReleasesOnlyAfterPhysicalReadDrains",
         "unauthenticatedRangeUsesServiceIdentityForOrdinaryProvider"},
    ("server", "surefire", "io.etcdhbase.server.commit.M5GatewayAuthenticationRpcTest"):
        {"authenticateOnAThenBRangeAndOwnerForwardingUseRevalidatedIdentityUntilPhysicalDrain",
         "noTokenPutAndTxnSucceedWithServiceIdentityWhenAuthenticationIsDisabled"},
    ("server", "surefire", "io.etcdhbase.server.commit.OwnerForwardingProtocolMapperTest"):
        {"roundTripsInvalidTokenFailureWithoutDowngradingItsCategory"},
    ("grpc", "surefire", "io.etcdhbase.grpc.protocol.ErrorProjectionTest"):
        {"keepsAuthenticationFailureDistinctFromPermissionDenied",
         "mapsAuthSessionFailuresToSafeClientErrors"},
    ("grpc", "surefire", "io.etcdhbase.grpc.RequestScopeInterceptorTest"):
        {"carriesEtcdTokenMetadataOnlyInsideTheRequestContext",
         "rejectsDuplicateEtcdTokenMetadataWithoutEnteringTheService"},
    ("hbase", "surefire", "io.etcdhbase.hbase.codec.AuthMessagesTest"):
        {"eachControlRecordRoundTripsWithoutSerializingBearerOrBackendHandle"},
}


J51_SCENARIOS = {
    "identity-retirement-and-acl-recheck",
    "single-group-revocation-recovery-and-publication-fencing",
    "deployment-registry-and-cross-group-revocation-mock",
    "queued-read-and-cache-invalidation",
    "lease-expiry-owner-handoff",
    "watch-and-keepalive-revalidation",
    "unsupported-auth-management-wire",
}
J51_REQUIRED_TESTS = {
    ("server", "surefire", "io.etcdhbase.server.AuthRegistryAdminCommandTest"):
        {"commandPersistsInventoryAndWorkersRecoverAnUnknownUpdateBeforeStatusChanges",
         "productionLeaseRequiresVersionedSecureIdentityAndIsolatedRecovery",
         "disabledGroupCanRecoverLaterConfigurationUpdates",
         "timedOutWriteKeepsControlResourcesUntilItsPhysicalProducerDrains",
         "inventoryRejectsDifferentControlBindingsAndDuplicateGroupsBeforeBackendAccess"},
    ("server", "surefire", "io.etcdhbase.server.AuthRegistryInventoryTest"):
        {"inventoryUsesTheRuntimeDatasetAndConfigurationBindings"},
    ("core", "surefire", "io.etcdhbase.core.auth.AuthDeploymentRegistryAdminTest"):
        {"explicitInitializationAndUpdateUseFullInventoryAndRejectInFlightRevocation",
         "reserveDoesNotPublishTheReplacementOrPermitAnotherOperation"},
    ("core", "surefire", "io.etcdhbase.core.auth.AuthConfigurationUpdateExecutorTest"):
        {"workersResumePartialFailureAndSwitchOnlyAfterAllOldGroupsAreFenced"},
    ("core", "surefire", "io.etcdhbase.core.auth.AuthDeploymentRegistryTest"):
        {"configurationReservationExcludesRevocationAndRequiresEveryBarrierBeforeSwitch"},
    ("hbase", "surefire", "io.etcdhbase.hbase.codec.AuthDeploymentRegistryMessagesTest"):
        {"preservesPartiallyInstalledConfigurationUpdate"},
    ("core", "surefire", "io.etcdhbase.core.auth.RegistryRevocationStoreTest"):
        {"reservationAndConfigurationUpdateCompeteOnTheSameRow",
         "staleReservationRetriesAfterAnotherOperationOrConfigurationUpdate"},
    ("core", "surefire", "io.etcdhbase.core.auth.RegistryAuthConfigurationStoreTest"):
        {"projectsDeploymentGenerationFromTheRegistry"},
    ("core", "surefire", "io.etcdhbase.core.auth.AuthorizationRevocationExecutorTest"):
        {"partialFailureKeepsTheSnapshotAndRestartResumesTheSameBarriers",
         "unknownCasOutcomeIsRecoveredFromTheSameDurableOperation",
         "workersForDifferentGroupsCooperateWithoutReleasingBeforeAllBarriers"},
    ("hbase", "surefire", "io.etcdhbase.hbase.storage.HBaseAuthDeploymentRegistryStoreTest"):
        {"usesStrongReadsAndExactDurableCasAndRejectsInvalidTransitions"},
    ("server", "surefire", "io.etcdhbase.server.auth.TargetAuthorizationBarriersTest"):
        {"partialFailureAndRestartFenceAndReleaseEveryRegisteredGroup"},
    ("server", "surefire", "io.etcdhbase.server.auth.DeploymentAuthorizationAdmissionTest"):
        {"blocksOnlyAffectedSubjectsAndRejectsStaleIncarnations",
         "rejectsAllSubjectsWhileConfigurationBarriersAreInstalling"},
    ("server", "surefire", "io.etcdhbase.server.auth.AuthorizationRevocationSnapshotTest"):
        {"snapshotsAllAndOnlyRegisteredFamilyScopesWithTheirIncarnations"},
    ("server", "surefire", "io.etcdhbase.server.auth.VersionedHBaseAuthenticatorTest"):
        {"configuredCredentialIsBoundToItsVersionAndDelegatesCleanup",
         "oldSecretCannotBePromotedToTheNewGeneration",
         "unconfiguredOrMissingVersionDoesNotBecomeAClaimedGeneration",
         "mismatchedBackendUserIsClosedWithoutAttestingAVersion"},
    ("server", "surefire", "io.etcdhbase.server.M3RuntimeGroupsTest"):
        {"secureGroupWaitsForRecoveryAndDrainsItsProducerBeforeClosing"},
    ("server", "surefire", "io.etcdhbase.server.auth.AuthDeploymentRecoveryWorkerTest"):
        {"retriesARecoveredOperationAfterFailure",
         "stopCancelsTheScopeAndWaitsForThePhysicalProducer",
         "cleanupFailureStopsRecoveryAndIsReportedToTheOwner"},
    ("core", "surefire", "io.etcdhbase.core.commit.AuthorizationBarrierInstallerTest"):
        {
            "allocatesTargetAfterCallerHasMadeRevocationDurableAndInstallsIt",
            "resolvesUnknownCasOnlyAfterStrongReadShowsTheTarget",
        },
    ("core", "surefire", "io.etcdhbase.core.commit.CommitPublisherTest"):
        {
            "installedRevocationBarrierRejectsASelectionUsingAnEarlierControlSnapshot",
            "configurationUpdateFencesAnAlreadyAuthorizedSelection",
            "selectedPublicationRemainsSelectedWhenRevocationInstallsBeforeCasReply",
            "timedOutSelectionIsResolvedAfterRevocationWithoutReauthorization",
            "deniedReauthorizationAfterBarrierAdvanceDoesNotAttemptAnotherCas",
        },
    ("core", "surefire", "io.etcdhbase.core.commit.ReadViewRecordStoreTest"):
        {
            "invalidationPreventsAnInFlightPhysicalReadFromRefillingTheCache",
            "invalidationRejectsAnInFlightCacheProof",
        },
    ("core", "surefire", "io.etcdhbase.core.kv.RangeResultCacheTest"):
        {
            "invalidationEvictsOnlyItsAuthorizationAndFencesEarlierFills",
            "entryCannotCrossAuthorizationOrBarrierAndHitSurvivesEvictionOnlyWhileCharged",
        },
    ("hbase", "surefire", "io.etcdhbase.hbase.identity.HBaseBackendIdentityProviderTest"):
        {
            "retirementStopsNewBorrowsAndWaitsForExistingBorrowToDrain",
            "identityCleanupFailureKeepsCapacityAndCanBeRetried",
            "accessRecheckUsesTheRealConnectionAndRetiresOnPermissionLoss",
        },
    ("server", "surefire", "io.etcdhbase.server.auth.AuthSessionServiceTest"):
        {
            "rotationResumesAfterAnUnknownAuthorityWriteAtEveryPersistedPhase",
            "rotationResumesAfterAnUnknownRegistryWriteAtEveryPersistedPhase",
            "failedBarrierInstallationIsRecoveredByAnotherRuntimeFromTheSameRegistry",
            "newerRotationFencesAnOlderRotationWaitingForBarrierCas",
            "deploymentRotationBlocksBothGroupsUntilTheirBarriersComplete",
            "independentGroupServicesResumeOnlyTheirOwnRegisteredBarriers",
            "localServicesCooperativelyRecoverConfigurationBarriers",
            "registryBindingMustMatchBeforeAuthenticationCanChangeAuthority",
            "authStatusConfigurationMustMatchThePersistedBridge",
        },
    ("server", "surefire", "io.etcdhbase.server.auth.ProviderSessionIdentityManagerTest"):
        {
            "explicitRetirementStopsNewBorrowsAndWaitsForExistingOnes",
            "rechecksReusedIdentityAndRejectsNewBorrowsAfterPermissionLoss",
        },
    ("server", "surefire", "io.etcdhbase.server.commit.SessionAuthorizationResolverTest"):
        {
            "userSessionCannotAuthorizeInternalLeaseExpiryStep",
            "ownerRetiresCapturedIdentityWhenPublicationRecheckFindsRevokedSession",
        },
    ("server", "surefire", "io.etcdhbase.server.commit.LocalLeaseExpiryAuthorizationResolverTest"):
        {
            "localCurrentOwnerCanResolveExpiryButCannotAuthorizeOtherMutations",
            "queuedExpiryIsFencedOnOwnerChangeAndNewOwnerCanContinue",
        },
    ("server", "surefire", "io.etcdhbase.server.commit.TxnGatewayAuthenticationTest"):
        {
            "queuedReadsOnBothGatewaysRejectSharedSessionRevocation",
            "queuedReadRevalidatesBeforeDeliveryAndRejectsRevokedSession",
        },
    ("server", "surefire", "io.etcdhbase.server.commit.LeaseKeepAliveStreamTest"):
        {
            "rejectsZeroTtlWhenIdentityRevalidationFails",
            "discardsValidationThatSpansAnOutboundNotReadyTransition",
        },
    ("server", "surefire", "io.etcdhbase.server.commit.WatchGatewayTest"):
        {
            "periodicRevalidationUsesResolvedIdentityAndDefersReleaseUntilSessionDrain",
        },
    ("server", "surefire", "io.etcdhbase.server.AuthManagementWireTest"):
        {
            "configuredAuthServiceRejectsEveryUnsupportedManagementMethodOverWire",
            "authStatusReturnsThePersistedBridgeRevisionAndUnavailableWhenMissing",
        },
}


class ReportError(Exception):
    """A task registry or evidence contract is invalid."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReportError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise ReportError(f"required JSON file is missing or unsafe: {path.name}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReportError(f"invalid JSON file: {path.name}") from error
    if not isinstance(value, dict):
        raise ReportError(f"JSON root must be an object: {path.name}")
    return value


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".m5-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _legal_task(task_id: object) -> str:
    if not isinstance(task_id, str) or task_id not in KNOWN_TASKS:
        raise ReportError(f"unknown M5 task ID: {task_id}")
    return task_id


def _load_task(task_id: str, registry: Path = REGISTRY) -> dict[str, object]:
    task_id = _legal_task(task_id)
    path = registry / f"{task_id}.json"
    task = _read_json(path)
    _validate_task(task, path, registry.parents[2])
    return task


def _validate_registry(registry: Path = REGISTRY) -> dict[str, dict[str, object]]:
    if registry.is_symlink() or not registry.is_dir():
        raise ReportError("M5 task registry directory is missing or unsafe")
    files = sorted(registry.iterdir())
    task_files: dict[str, Path] = {}
    for path in files:
        if path.is_symlink() or not path.is_file() or path.suffix != ".json":
            raise ReportError(f"unexpected or unsafe M5 registry entry: {path.name}")
        task_id = path.stem
        _legal_task(task_id)
        if task_id in task_files:
            raise ReportError(f"duplicate registered M5 task: {task_id}")
        task_files[task_id] = path
    result: dict[str, dict[str, object]] = {}
    for task_id, path in task_files.items():
        task = _read_json(path)
        if task.get("task") != task_id:
            raise ReportError(f"registry filename and task ID disagree: {path.name}")
        _validate_task(task, path, registry.parents[2])
        result[task_id] = task
    return result


def _validate_task(task: dict[str, object], path: Path, repo: Path) -> None:
    if set(task) != TASK_KEYS:
        raise ReportError(f"task fields are missing or unknown in {path.name}")
    task_id = _legal_task(task.get("task"))
    if (task.get("schema") != TASK_SCHEMA or task.get("status") != "executable"
            or task.get("scope") != "public"
            or task.get("maven_reactor") != "server"):
        raise ReportError(f"task metadata is invalid in {path.name}")
    profile = task.get("execution_profile")
    if not isinstance(profile, str) or profile != f"m5-{task_id.lower()}-public":
        raise ReportError(f"execution profile is missing or mismatched in {path.name}")
    if task_id == "J50":
        _validate_j50(task, path, repo)
    elif task_id == "J51":
        _validate_j51(task, repo)
    elif task_id == "J52":
        _validate_j52(task, repo)
    else:
        raise ReportError(f"registered task lacks an implemented verifier contract: {task_id}")


def _validate_j50(task: dict[str, object], path: Path, repo: Path) -> None:
    if task.get("backend_adapters") != [
            "kerberos-password", "delegation-token", "password-sasl"]:
        raise ReportError("J50 must register each real backend authentication adapter")
    if task.get("layouts") != ["auth-control", "secure-hbase-data"]:
        raise ReportError("J50 layout registration changed or is incomplete")
    scenarios = task.get("scenarios")
    if (not isinstance(scenarios, list) or len(scenarios) != len(set(scenarios))
            or set(scenarios) != J50_SCENARIOS):
        raise ReportError("J50 required scenario registration is missing, duplicated, or unknown")

    profiles = task.get("maven_profiles")
    expected_profiles = [{"module": "hbase", "id": "m5-j50-public"},
                         {"module": "server", "id": "m5-j50-public"}]
    if profiles != expected_profiles:
        raise ReportError("J50 must declare exactly the hbase and server execution profiles")
    for entry in expected_profiles:
        _validate_maven_profile(repo, entry["module"], entry["id"])

    found = _validate_suites(task, repo, "J50", J50_REQUIRED_TESTS, {"grpc", "hbase", "server"})
    if ("hbase", "failsafe", "io.etcdhbase.hbase.storage.HBaseAuthControlIT") not in found:
        raise ReportError("J50 must execute the real auth-control storage integration test")
    if ("server", "failsafe", "io.etcdhbase.server.M5AuthProcessIT") not in found:
        raise ReportError("J50 must execute the independent secure process integration test")


def _validate_j51(task: dict[str, object], repo: Path) -> None:
    if task.get("backend_adapters") != ["mock-identity"]:
        raise ReportError("J51 backend adapter registration changed or is incomplete")
    if task.get("layouts") != ["mock-storage"]:
        raise ReportError("J51 layout registration changed or is incomplete")
    scenarios = task.get("scenarios")
    if (not isinstance(scenarios, list) or any(not isinstance(value, str) for value in scenarios)
            or len(scenarios) != len(set(scenarios)) or set(scenarios) != J51_SCENARIOS):
        raise ReportError("J51 required scenario registration is missing, duplicated, or unknown")
    if task.get("maven_profiles") != []:
        raise ReportError("J51 mock gate must not require a real-cluster Maven profile")
    _validate_suites(task, repo, "J51", J51_REQUIRED_TESTS, {"core", "hbase", "server"})


J52_REQUIRED_TESTS = {
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.SystemRecordGcTargetTest'): {
        'metadataOnlyDecisionSharesItsPreparedResultWindowRatherThanBlockingThePreviousRevision',
        'keyChangingAndTerminalDecisionsKeepTheirActualPublicationBounds',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.CompactForwarderTest'): {
        'logicalReceiptWaitsForPhysicalDrainAndCannotHideCleanupFailure',
        'boundedCommandPreservesNegativeZeroAndPhysicalWithoutProtoUnknownFields',
        'returnedOperationMustMatchInvocationRequestAuthorizationProfileRevisionAndTerminalStage',
        'unknownSendOutcomeNeverAllocatesAnotherAttempt',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.CompactOwnerLaneTest'): {
        'compactUsesItsOwnOperationReceiptAndNeverCreatesAnInvocationBinding',
        'peerContextGroupHopAndEpochAreCheckedBeforeSelection',
        'queuedCompactRechecksOwnerAndCancellationCannotDispatchIt',
    },
    ('core', 'surefire', 'io.etcdhbase.core.commit.InvocationResultResolverTest'): {
        'resultQueryProtectionAcceptsTerminalRevisionAndSequence',
        'failedProducerDrainKeepsThePinAndSurfacesCleanupFailure',
        'resolvesLostTransitionAcknowledgementAndReplaysTheSamePersistedBytes',
        'refusesMissingResultWrongAuthorizationAndClosedQueryWindow',
        'replaysTheSameNoChangeResultWithoutReadingACommitDecision',
        'resolvesEitherDurableInvocationOutcomeForProtocolProjection',
        'noChangeReplayRejectsMissingPagesAndAWindowClosedDuringLoad',
        'transactionResolutionDistinguishesNoChangeFromMetadataOnlyPublication',
        'selectedAndNoChangeQueriesHoldDurableFloorsUntilMaterialization',
        'gcWinningEitherRegistrationFloorPreventsAnyResultRead',
        'expiredOrRevokedQueryCannotDeliverMaterializedBytes',
        'cancelledQueryKeepsDurableProtectionUntilTheActualProducerStops',
    },
    ("hbase", "surefire", "io.etcdhbase.hbase.storage.J52RetentionDescriptorTest"): {
        "rejectsFiniteTtlForEveryLayout",
        "rejectsSingleVersionForNativeLayout",
        "historyDescriptorDefaultsToOneVersionAndForeverTtl",
        "nativeDescriptorDefaultsTo64AndAccepts128Versions",
    },
    ("core", "surefire", "io.etcdhbase.core.commit.CommitStateTest"): {
        "authorizationBarrierAdvancesMonotonicallyWithoutReplacingSelectedPublication",
        "ownerEpochFencesOldOwnersAndExpiredLeaseCanBeClaimed",
    },
    ("core", "surefire", "io.etcdhbase.core.commit.SnapshotPinManagerTest"): {
        "durableFloorsProtectExistingViewsAndRejectNewOlderViews",
        "registrationRacesFloorAdvanceOnTheSameShardCas",
        "completionCasLosingToDurableRevocationCannotReportSuccess",
    },
    ("core", "surefire", "io.etcdhbase.core.commit.EventIndexMaterializerTest"): {
        "persistsSelectedIndexAndReplaysIdentically",
    },
    ("core", "surefire", "io.etcdhbase.core.commit.EventIndexBackfillMaterializerTest"): {
        "initialPreflightStillRejectsOwnerCompactAndCapturedUpperChanges",
    },
    ("hbase", "surefire", "io.etcdhbase.hbase.storage.HBaseMaterializationStoreTest"): {
        "requiresSystemAndDataTablesToBelongToTheSameProvisionedGroup",
    },
    ("hbase", "surefire", "io.etcdhbase.hbase.storage.HBaseCommitControlStoreTest"): {
        "authorizationBarrierTransitionIsIndependentOfEventRegistryFormat",
    },
    ("hbase", "surefire", "io.etcdhbase.hbase.codec.SystemRecordKeyCodecTest"): {
        "binaryIdentityRoundTripsForEveryRecordDomain",
    },
    ('core', 'surefire', 'io.etcdhbase.core.commit.CompactionServiceTest'): {
        'deniedAuthorizationCannotCreateACompactionRecord',
        'authorizationBarrierRaceRevalidatesAndUsesExactFullState',
        'matchesNativeZeroRepeatedFutureAndNegativeBoundariesWithoutKvCommit',
        'ownerTakeoverAndInvocationConflictCannotSelectAPreparedOperation',
        'permissionFailureIsNotHiddenByReadbackAndCancellationStopsNewSelection',
        'persistsPreviousSelectionBeforeReplacingItsControlIdentity',
        'recoversUnknownCreateControlAndOperationAcknowledgements',
        'refreshesResponseBarrierAfterConcurrentControlChange',
        'strictRecordCodecRejectsMalformedAndUnknownStages',
        'refusesLegacyControlBeforeCreatingAnOperationWithoutAnAuthorizationBarrier',
        'unknownCasBeforeWriteDoesNotBecomeSuccessFromAHigherUnrelatedCompact',
    },
    ('core', 'surefire', 'io.etcdhbase.core.commit.GcCoordinatorTest'): {
        'requiresConfirmedLogicalOperationAndBothIndependentPinFloors',
        'staleOwnerCannotObtainDeletionPermissionEvenAfterLogicalSelection',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.codec.CommitControlMessagesTest'): {
        'authorizationBarrierPresenceIsIndependentOfControlFormatAndEventRegistry',
        'auxiliaryControlFormatRoundTripsWithoutChangingLegacyBytes',
        'compactIdentitySurvivesOwnerPublicationAndAuthorizationUpdates',
        'decoderRejectsControlMessagesAboveTheByteLimit',
        'initialAndPendingPublicationStatesRoundTrip',
        'manifestRootAcceptsItsMaximumAndRejectsTheNextByte',
        'missingRequiredFieldsAndCorruptNestedValuesAreRejected',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.SystemTableBindingTest'): {
        'acceptsTheExactDurableSingleVersionSystemTable',
        'resultCapabilityIsExplicitAndDoesNotChangeLegacyValidation',
        'compactRecordsRequireExplicitDescriptorCapability',
        'gcFencingRequiresAnExplicitDurableIsolatedFamily',
        'rejectsTableFamilyProfileCategoryAndDurabilityMismatches',
        'rejectsExpiringOrMultiVersionFamilies',
        'rejectsDifferentRoutingParametersWithTheSameProfileId',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanTest'): {
        'rejectsGcActivationAndWritesBeforeUsingAnOlderTable',
        'cancelledScanRetainsPhysicalOwnershipUntilHBaseSignalsCompletion',
        'validatesPhysicalRowOrderForLengthFramedIdentities',
        'rejectsBackwardPhysicalRowsEvenWhenLogicalIdentitiesAreIncreasing',
        'rejectsCompactRecordsBeforeTouchingAnUndeclaredTable',
        'rejectsAuxiliaryAccessBeforeTouchingAnOlderSystemTable',
        'rejectsLeaseBindingsWhenOnlyJ40AuxiliaryCapabilityIsPresent',
        'rejectsEventIndexWhenOnlyLeaseExpiryCapabilityIsPresent',
    },
    ('hbase', 'failsafe', 'io.etcdhbase.hbase.storage.J52HistoryCollectionIT'): {
        'nativeAdmissionPreservesPinnedHistoryWhileRawVersionEvictionIgnoresPins',
        'finiteTtlExpiresWallClockCellsAndImmediatelyHidesRevisionTimestampsInBothLayouts',
        'unfencedLatePutCanReappearAfterItsDeleteMarkerIsCompacted',
        'activeReadPinPreventsDeletionUntilReleaseInBothLayouts',
        'enumeratesEveryKeyWithBucketLocalExclusiveResumePoints',
        'boundsKeyEnumerationToOneDurableBucketInBothLayouts',
        'doesNotEndEnumerationInsideAHighChurnKey',
        'resumesAcrossSaltBucketsWithoutComparingGlobalLogicalOrder',
        'cancellationRetainsPhysicalProducerUntilLateDeleteDrains',
        'preservesPinnedBaselineTombstoneNewerWritesAndRetiredGuardsInBothLayouts',
        'rejectsPermissionFromAnotherPinRegistryBeforeDeletingData',
        'rejectsCompactControlWithoutDescriptorCapabilityAndDuringInitialization',
        'resolvesLostDeleteAcknowledgementAndReplansWhenANewerNativeWriteWins',
    },
    ('core', 'surefire', 'io.etcdhbase.core.commit.SystemRecordRetirementPermitTest'): {
        'derivedPermitOnlyNarrowsBoundsAndKeepsAuthorityProtectionsAndScope',
        'ordinaryCallersCannotMintRetirementPermits',
        'permitBindsProfileRegistryOperationGenerationAndBoundary',
        'permitFailsClosedForAnotherProfileScopeOrAfterItsScopeCloses',
        'permitRejectsAnUnaccountedGenerationOrNegativeBoundary',
        'tombstoneRequiresAnExactDigestAndAccountedBoundary',
    },
    ('core', 'surefire', 'io.etcdhbase.core.commit.SystemRecordPageTest'): {
        'preservesStorageOrderForLengthFramedIdentities',
        'rejectsDuplicateIdentitiesRegardlessOfStorageEncoding',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.HBaseSystemRecordDecodeTest'): {
        'rejectsOversizedCellBeforeCopyingItsValue',
        'aMalformedRetirementPayloadCannotBeReportedAsAbsent',
        'acceptsExpectedSingleCell',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.HBaseSystemRecordRetirementApiTest'): {
        'publicStoreApiCannotBypassTheClosureDecision',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.HBaseSystemRecordClosureTest'): {
        'resultPageChecksOnlyItsCanonicalDirectoryAndRejectsDifferentOrMalformedPreparations',
        'proofBudgetEndsAtTheCompletedPrefixAndTheNextPageRetriesTheUnprovedRecord',
        'anUnprovableFirstRecordStillFailsRatherThanPublishingANonadvancingCursor',
        'malformedRecordAfterACompletedPrefixStillAbortsThePage',
        'metadataOnlyDecisionStaysOutsideThePreviousWindowAndCollectsAfterItsReferencesDrain',
        'newerManifestPagesUseRootOrPermanentInvocationBoundsWithoutBlockingOldCompletion',
        'exactRootProtectionCountsAsPendingEvenAboveNumericFloors',
        'changedRetirementOutcomeRemainsPending',
        'terminalPreparedCountersRemainAtTheNonRetireableUpperBound',
        'retainsCandidateReferencedByManifestPage',
        'retainsEventReferencedByEventPage',
        'retainsResultReferencedByManifestRoot',
        'followsReferencePagesIncludingAnEmptyTombstonePage',
        'resumesFromPhysicalCursorWithoutRestartingEarlierBuckets',
        'aFullLastBucketCanStillBeExhausted',
        'preparingInvocationProtectsRecordsBeforeItsManifestIsWritten',
        'closedInvocationDoesNotRetainItsTransactionForever',
        'unselectedInvocationDoesNotProtectAbandonedTransactionRecords',
        'lookaheadRowsDoNotMakeTheRemainingPageBudgetNegative',
        'byteLimitedPageDoesNotEndCollectionBeforeTheContinuation',
    },
    ('hbase', 'failsafe', 'io.etcdhbase.hbase.storage.HBaseSystemRecordRetirementIT'): {
        'retirementAcknowledgementRequiresStrongTombstoneReadbackBeforeSuccess',
        'lostWindowCloseAcknowledgementIsConfirmedByTheDurableClosedBinding',
        'successorRowFenceRejectsAnAlreadyDispatchedWindowClose',
        'descriptorChangesAfterOpenRejectFenceAndRetirementWithoutMutations',
        'legacyCompactTableRejectsGcWithoutAutomaticSchemaMutation',
        'fencePreservesLegacyPayloadGetScanAndConditionalWriteLayouts',
        'lostFenceAcknowledgementAndPartialRestartNeverActivateAnIncompleteShard',
        'lateRetirementIsFencedBeforeSuccessorStartsCollection',
        'retirementReplacesTheValueAndBlocksEveryRecreationPath',
        'retirementIsIdempotentForTheSamePlanAndRejectsAChangedValue',
        'repeatedCreateCannotOverwriteAnExistingImmutableRecord',
        'absentCasCannotReplaceALiveValue',
        'staleRetirementCannotDeleteAReplacementValue',
        'bucketScanAdvancesAcrossTombstonesAndReturnsFollowingLiveRecord',
        'permitIsUnusableAfterItsScopeClosesAndForAnotherProfile',
        'staleOwnerCannotRetireOrCloseAnInvocationWindow',
        'durableRegistryDamageRejectsAnAlreadyIssuedPermit',
    },
    ('hbase', 'failsafe', 'io.etcdhbase.hbase.storage.HBaseSystemRecordClosureIT'): {
        'keepsAReferencedManifestAndCollectsAnUnreferencedOne',
        'corruptReferenceStopsPageWithoutRetirement',
        'manifestPagesAndRootsProtectRealCandidateEventAndResultRecords',
    },
    ('hbase', 'failsafe', 'io.etcdhbase.hbase.storage.InvocationWindowCloserIT'): {
        'retainsResolvedWindowsAtTheTerminalCounters',
        'runtimeClosureWaitsForADurableResultQueryThenKeepsTheNonReplayBinding',
        'closesAFinalizedSealedInvocationWithoutRequiringAClientReplay',
        'closesTheWindowFirstAndKeepsTheDigestsNeededToRejectAReplay',
        'anUnresolvedInvocationIsRetainedRatherThanClosed',
        'closingAWindowRetainsAResultStillReferencedByAManifest',
    },

    ('core', 'surefire', 'io.etcdhbase.core.commit.GcCheckpointStoreTest'): {
        'rejectsNegativePendingAndInvalidCompletionReceipts',
        'pendingPrefixSurvivesTakeoverAndCleanEofDoesNotPublishCompletion',
        'completionSurvivesRestartStrongerCompactAndOwnerTakeover',
        'zeroBoundsNeedAPositiveCompletedGeneration',
        'rejectsChangedRegistryBindingAndReusedGeneration',
        'durableScanAndFenceCursorsMustBelongToTheNamedBucket',
        'initialLeaseAndTakeoverRequireTheWholeDurableFencePass',
        'wideningBoundsDuringOneCompactDoesNotResetAnUnfinishedSweep',
        'roundTripsEveryDomainAndMaximumGroupAndCursor',
        'rejectsUnknownPhaseMalformedCursorsUtf8AndTrailingBytes',
        'rejectsTopologyChangesOnResumeAndTakeover',
        'sameEpochResumesAndTakeoverPreservesProgressWhileStaleCasLoses',
        'exhaustedSweepAndNewOperationRevisitPrefixesWhileWiderBoundsWait',
        'rejectsRowValueShardMismatchInClaimAndRead',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.GcCheckpointCycleTest'): {
        'checkpointWaitsForPhysicalProducerAndCleanupBeforeMetadataCas',
        'failedProducerOrCleanupCannotPublishCheckpointProgress',
        'durableOwnerChangeDuringDrainCannotAdvanceEvenWithAnUnchangedLocalToken',
        'failedFencePageCannotAdvanceOrStartRetirement',
        'metadataLeaseRunsOnlyTheFencePhaseBeforeCollection',
        'resumedSweepUsesOnlyANarrowerDerivativeOfTheFreshAuthority',
        'aBusyBucketYieldsTheNextPageToTheNextShard',
        'nonOwnerNeverAdmitsOrTouchesStorage',
        'absentCompactWaitsWithoutCallingAuthorityOrCreatingCheckpoints',
        'regularAdmissionFailureYieldsAndRecordsGcBacklogWithoutUsingReserve',
        'authorizationFailureCannotCreateOrAdvanceACheckpoint',
        'ownerLossAfterCollectionCannotAdvanceDurableProgressAndAdmissionWaitsForProducer',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.GcCheckpointWorkerTest'): {
        'queuedCycleDoesNotStartIoAfterStop',
        'stopWaitsForPhysicalProducerAndActualCleanupAfterLogicalCompletion',
        'cancellationReachesLogicalIoButDoesNotPretendPhysicalDrain',
        'physicalAndCleanupFailuresSurviveStop',
        'executorRejectionClosesScopeAndRetriesWithoutLeakingOutstandingWork',
        'schedulerRejectionIsVisibleAndStopBeforeStartCannotResurrectWorker',
    },
    ('server', 'failsafe', 'io.etcdhbase.server.commit.GcCheckpointCycleIT'): {
        'durableCompletionNeedsAnotherSweepAfterTheReferenceChainDrains',
        'realWorkerProtectsPinnedEventsThenRestartCollectsBothBucketsAndFencesOldCheckpoint',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.M3TableGroupRuntimeWorkerLifecycleTest'): {
        'ownerLossStopsInstalledGcAndRejectsReplacement',
        'historyMaintenanceRunsWithoutLeaseAndOwnerLossRejectsLateInstallation',
        'failedHistoryStopStillClosesWaitersSystemGcAndTailerOnOwnerLoss',
    },
    ('hbase', 'surefire', 'io.etcdhbase.hbase.storage.SystemRecordGcFenceTest'): {
        'newerOwnerDominatesEveryOlderCompactGeneration',
        'rejectsCorruptDuplicateOrUnknownMaintenanceCells',
        'payloadReaderKeepsTheBaseLayoutAndRejectsInlineFences',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.CompactGatewayAuthorizationTest'): {
        'forwardedCompactResolvesTheSessionAndRechecksUserAdminBeforeBothWrites',
        'missingAndInvalidTokensCannotCreateOperationsOrAdvanceControl',
        'revokedSessionIsRejectedBeforeCreatingAnOperation',
        'ordinaryUserCannotCompactEvenWhenPhysicalCollectionIsUnavailable',
        'authenticationDisabledStillRequiresBackendAdminPermission',
        'unavailableManagementPermissionCheckCannotPersistWork',
        'authorizedManagerSelectsWithItsOwnIdentityAndChecksEveryTableTwice',
        'managementPermissionRevokedBeforeSelectionCannotAdvanceControl',
        'sessionRevokedBeforeSelectionCannotAdvanceControl',
        'cancellationRetainsUserBorrowUntilLateConnectionDrains',
        'cancellationRetainsUserBorrowUntilLatePermissionRpcDrains',
        'rpcMetadataSelectsTheRequestSessionInsteadOfTheBackgroundIdentity',
    },
    ('core', 'surefire', 'io.etcdhbase.core.commit.HistoryGcCheckpointStoreTest'): {
        'encodingDistinguishesAnEmptyBusinessKeyFromAnAbsentCursor',
        'rejectsTruncatedOversizedAndTrailingCheckpointBytes',
        'restartResumesTheExactActiveVersionPageAfterAnUnknownIntentAcknowledgement',
        'incompleteVersionsCannotAdvanceTheKeyOrPublishCompletion',
        'aNewOwnerFencesOldProgressButKeepsTheConfirmedCursor',
        'strongerFloorsWaitForTheNextSweepAndKeepThePreviousCompletionReceipt',
        'foreignBucketKeysCannotBecomeDurableWorkOrCompletion',
        'permitRestrictionCannotWidenBoundsOrEscapeItsScope',
        'aDifferentProfileOrPinRegistryCannotClaimHistoryProgress',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.HistoryGcCycleTest'): {
        'zeroRevisionPersistsCompletionWithoutEnumeratingANonemptyBucket',
        'ownerLossBeforeZeroRevisionClaimReturnsCannotPublishCompletion',
        'persistsActiveKeyIntentBeforeAnyDeletionCanStart',
        'resumesOnlyOneConfirmedVersionPageUnderTheFrozenSafeBounds',
        'unknownDeletionCannotAdvanceTheDurableVersionCursor',
        'onlyBackendEofAfterTheLastCompleteKeyPublishesTheBucketWatermark',
        'ownerLossBeforeDeleteReadbackCannotPublishProgress',
        'maintenanceAdmissionDoesNotReleaseUntilTheActualProducerDrains',
        'unavailableMaintenanceCapacityStartsNoStorageWork',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.LeaseExpiryWorkerTest'): {
        'completedEmptyWindowWaitsForTheIdleIntervalBeforePublishingAgain',
        'cleanupExecutorRejectionStillWaitsForPhysicalProducerDrain',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.PhysicalGcCompletionScanTest'): {
        'kvCompletionAloneCannotCompleteAnySystemDomain',
        'everyDomainAndBucketNeedsBothTargetBounds',
        'eofAndDefaultZeroBoundsAreNotCompletionReceipts',
        'profileRegistriesNamespaceAndTopologyCannotTransferCompletion',
        'durableOlderReceiptSurvivesANewerPendingSweepAndOwnerTakeover',
        'storageFailureAndForeignTargetCannotBecomeCompletion',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.HistoryGcCompletionScanTest'): {
        'conservativePinBoundsDoNotCompleteTheRequestedTarget',
        'defaultZeroCountersAreNotACompletedZeroRevisionSweep',
        'bothRevisionAndSequenceAndEveryBucketMustBeConfirmed',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.PhysicalCompactionWaiterTest'): {
        'waitsForDurableProgressAndGivesEachCallerAnIndependentScan',
        'cancellationDetachesTheRpcButRetainsAdmissionUntilTheRealReadDrains',
        'deadlineTerminatesEvenWhenTheReadFutureNeverResponds',
        'successWaitsForCleanupAndCleanupFailureCannotFollowASuccessResponse',
        'shutdownRejectsWaitersAndPreventsQueuedPollsFromStartingStorageWork',
    },
    ('server', 'failsafe', 'io.etcdhbase.server.J52PhysicalCompactIT'): {
        'standbyForwardsCompactOverMtlsAndKeepsWaitingAcrossOwnerTakeoverInBothLayouts',
        'kvOnlyRuntimeCannotReportFullPhysicalCompletion',
        'eventProtectionKeepsPhysicalRpcWaitingAfterKvCompletionInBothLayouts',
        'zeroRevisionOnANonemptyTablePersistsCompletionWithoutBusinessScansInBothLayouts',
        'physicalRpcWaitsForAnOldPinThenConfirmsEveryEligibleVersionInBothLayouts',
        'cancelledPhysicalRpcResumesAfterOwnerProcessDeathAndLogicalOnlyRegistration',
    },
    ('server', 'surefire', 'io.etcdhbase.server.commit.CompactGatewayTest'): {
        'nonOwnerForwardsOutsideTheLocalLaneAndWaitsForItsOwnReceipts',
        'emptyEncodedZeroRequestCanEnterTheRealOwnerLane',
        'realLaneRejectionReturnsAnErrorInsteadOfHanging',
        'dispatchedServiceScopeCannotSendSuccessAfterTheRpcScopeIsCancelled',
        'realLaneSuccessWaitsForServiceCleanup',
        'realLaneCleanupFailureCannotFollowAnAlreadySentSuccess',
        'logicalSelectionReturnsTheResponseRevisionAndItsServingHeader',
        'physicalWaitDoesNotHoldTheOwnerLane',
        'physicalCompactWaitsForItsOwnBoundedHistoryDeletion',
        'physicalCompactReportsFailureWhenHistoryDeletionFails',
        'physicalCompactFailsClosedWhenNoHistoryCollectorIsWired',
        'logicalCompactStillSucceedsWithoutAHistoryCollector',
        'repeatedCompactIsReportedAsCompactedRatherThanSucceeding',
        'futureCompactIsRejectedWithoutAdvancingTheBoundary',
        'missingOwnerFailsClosedWithoutAttemptingAControlWrite',
        'cancelledRequestDoesNotSendAResponse',
    },

}


def _validate_j52(task: dict[str, object], repo: Path) -> None:
    if task.get("backend_adapters") != ["minicluster-simple", "minicluster-kerberos"] or task.get("layouts") != ["HISTORY", "NATIVE"]:
        raise ReportError("J52 storage gate must cover both real MiniCluster layouts")
    expected_scenarios = {
        "logical-selection-and-unknown-outcome", "independent-pin-floors",
        "exact-kv-version-delete-and-baseline-retention", "late-materializer-new-write-and-cancellation",
        "native-v3.5.23-compact-range-watch-oracle",
        "durable-system-retirement-and-reference-closure",
        "invocation-window-close-before-result-retirement",
        "bucket-checkpoint-restart-pin-retention-and-physical-drain",
        "durable-system-domain-completion-and-drained-checkpoint",
        "durable-kv-bucket-version-progress-and-target-completion",
        "physical-rpc-old-pin-owner-lane-and-exact-delete",
        "cancelled-rpc-owner-process-death-and-logical-only-resume",
        "complete-kv-and-system-receipts-with-event-pin-blocking",
        "bounded-proof-prefix-and-canonical-result-directory",
        "mtls-compact-forwarding-owner-takeover-and-drained-identity",
        "native-capacity-admission-and-raw-pinned-version-eviction",
        "finite-ttl-wall-clock-and-revision-timestamp-expiry",
        "actual-major-compaction-active-hfiles-and-late-put-guards",
    }
    scenarios = task.get("scenarios", [])
    if (not isinstance(scenarios, list) or any(not isinstance(value, str) for value in scenarios)
            or len(scenarios) != len(set(scenarios)) or set(scenarios) != expected_scenarios):
        raise ReportError("J52 storage scenarios are incomplete")
    if task.get("maven_profiles") != [
            {"module": "hbase", "id": "m5-j52-public"},
            {"module": "server", "id": "m5-j52-public"}]:
        raise ReportError("J52 must execute its real storage integration profile")
    _validate_maven_profile(repo, "hbase", "m5-j52-public")
    _validate_maven_profile(repo, "server", "m5-j52-public")
    found = _validate_suites(task, repo, "J52", J52_REQUIRED_TESTS, {"core", "hbase", "server"})


def _verify_j52_oracle(commands: list[dict[str, object]], artifact_root: Path) -> None:
    oracle = [entry for entry in commands if entry.get("name") == "native-oracle"]
    if len(oracle) != 1 or oracle[0].get("exit_code") != 0:
        raise ReportError("J52 requires a successful native oracle command")
    path = artifact_root / str(oracle[0].get("stdout"))
    if path.is_symlink() or not path.is_file():
        raise ReportError("J52 native oracle report is missing")
    try:
        events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (ValueError, UnicodeError) as error:
        raise ReportError("J52 native oracle report is invalid") from error
    if any(not isinstance(event, dict) for event in events):
        raise ReportError("J52 native oracle events must be objects")
    expected = {"TestM5CompactPinnedNativeBoundaries"} | {
        f"TestM5CompactPinnedNativeBoundaries/watch/{revision}" for revision in (3, 4, 5, 0, -1)
    }
    passed = {event.get("Test") for event in events if event.get("Action") == "pass"}
    if not expected.issubset(passed) or any(event.get("Action") in {"skip", "fail"} for event in events):
        raise ReportError("J52 native oracle tests are missing, failed or skipped")


def _validate_suites(task: dict[str, object], repo: Path, task_id: str,
                     required_tests: dict[tuple[str, str, str], set[str]],
                     allowed_modules: set[str]) -> dict[tuple[str, str, str], set[str]]:

    tests = task.get("tests")
    if not isinstance(tests, list) or not tests:
        raise ReportError(f"{task_id} must register one or more non-empty test suites")
    found: dict[tuple[str, str, str], set[str]] = {}
    for entry in tests:
        if not isinstance(entry, dict) or set(entry) != TEST_KEYS:
            raise ReportError(f"{task_id} test suite has missing or unknown fields")
        module, kind, class_name, methods = (
            entry.get("module"), entry.get("kind"), entry.get("class"), entry.get("methods"))
        if (not isinstance(module, str) or module not in MODULES or module not in allowed_modules
                or not isinstance(kind, str) or kind not in TEST_KINDS
                or not isinstance(class_name, str)
                or not re.fullmatch(r"[A-Za-z0-9_.]+", class_name)
                or not isinstance(methods, list) or not methods
                or any(not isinstance(method, str) or not re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*", method)
                       for method in methods)
                or len(methods) != len(set(methods))):
            raise ReportError(f"{task_id} test suite has an invalid module, class, kind, or method list")
        key = (module, kind, class_name)
        if key in found:
            raise ReportError(f"duplicate registered test suite: {class_name}")
        found[key] = set(methods)
    for key, methods in required_tests.items():
        if not methods.issubset(found.get(key, set())):
            raise ReportError(f"{task_id} required test methods are missing: {key[2]}")
    for (module, _kind, class_name), methods in found.items():
        source = repo / module / "src/test/java" / Path(*class_name.split(".")).with_suffix(".java")
        if source.is_symlink() or not source.is_file():
            raise ReportError(f"registered {task_id} test source is missing or unsafe: {class_name}")
        try:
            source_text = source.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise ReportError(f"registered {task_id} test source cannot be read: {class_name}") from error
        absent = sorted(method for method in methods
                        if not re.search(rf"\bvoid\s+{re.escape(method)}\s*\(", source_text))
        if absent:
            raise ReportError(f"registered {task_id} methods are absent from source: {class_name}: {absent}")
    return found


def _validate_maven_profile(repo: Path, module: str, profile_id: str) -> None:
    pom = repo / module / "pom.xml"
    if pom.is_symlink() or not pom.is_file():
        raise ReportError(f"Maven profile POM is missing or unsafe: {module}")
    try:
        root = ET.parse(pom).getroot()
    except ET.ParseError as error:
        raise ReportError(f"Maven POM is malformed: {module}") from error
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    profiles = root.findall("m:profiles/m:profile", namespace)
    matching = [profile for profile in profiles
                if profile.findtext("m:id", namespaces=namespace) == profile_id]
    if len(matching) != 1:
        raise ReportError(f"required Maven execution profile is absent or duplicated: {module}/{profile_id}")
    plugin = matching[0].find(
        "m:build/m:plugins/m:plugin[m:artifactId='maven-failsafe-plugin']", namespace)
    if plugin is None:
        raise ReportError(f"execution profile has no Failsafe gate: {module}/{profile_id}")
    configuration = plugin.find("m:configuration", namespace)
    if configuration is None or configuration.findtext("m:skipITs", namespaces=namespace) != "false":
        raise ReportError(f"execution profile can silently skip integration tests: {module}/{profile_id}")
    includes = {include.text for include in configuration.findall("m:includes/m:include", namespace)}
    required = {
        ("m5-j50-public", "hbase"): {"**/HBaseAuthControlIT.java"},
        ("m5-j50-public", "server"): {"**/M5AuthProcessIT.java", "**/M5AuthenticateGatewayProcessIT.java"},
        ("m5-j51-public", "server"): {"**/M5AuthenticateGatewayProcessIT.java"},
        ("m5-j52-public", "server"): {"**/GcCheckpointCycleIT.java", "**/J52PhysicalCompactIT.java"},
        ("m5-j52-public", "hbase"): {
            "**/J52HistoryCollectionIT.java", "**/HBaseSystemRecordRetirementIT.java",
            "**/HBaseSystemRecordClosureIT.java", "**/InvocationWindowCloserIT.java",
        },
    }.get((profile_id, module), set())
    if not required.issubset(includes):
        raise ReportError(f"execution profile does not include required IT: {module}/{profile_id}")
    if (module == "server" and profile_id in {"m5-j50-public", "m5-j51-public", "m5-j52-public"}
            and configuration.findtext("m:reuseForks", namespaces=namespace) != "false"):
        raise ReportError(f"{profile_id[3:6].upper()} server profile must isolate Kerberos MiniKDC tests by JVM")


def _validate_all(registered: dict[str, dict[str, object]]) -> None:
    missing = sorted(KNOWN_TASKS - registered.keys())
    if missing:
        raise ReportError("M5 --all is unavailable; missing task profiles: " + ", ".join(missing))


def _maven_selectors(task: dict[str, object]) -> tuple[str, str]:
    surefire: set[str] = set()
    failsafe: dict[str, set[str]] = {}
    for entry in task["tests"]:
        class_name = entry["class"].rsplit(".", 1)[-1]
        if entry["kind"] == "surefire":
            surefire.add(class_name)
        else:
            failsafe.setdefault(class_name, set()).update(entry["methods"])
    integration = [f"{name}#{'+'.join(sorted(methods))}"
                   for name, methods in sorted(failsafe.items())]
    return ",".join(sorted(surefire)), ",".join(integration)


def _git(repo: Path, *arguments: str) -> str:
    completed = subprocess.run(["git", "-C", str(repo), *arguments],
                               check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise ReportError("could not capture source control metadata")
    return completed.stdout.rstrip("\n")


def _source_snapshot(repo: Path) -> dict[str, object]:
    head = _git(repo, "rev-parse", "HEAD")
    tree = _git(repo, "rev-parse", "HEAD^{tree}")
    parents = _git(repo, "show", "-s", "--format=%P", "HEAD").split()
    status = _git(repo, "status", "--porcelain=v1", "--untracked-files=all")
    changed: list[dict[str, str]] = []
    excluded = (".artifacts/", "target/", "/target/", "scripts/verify/__pycache__/", ".pyc")
    for line in status.splitlines():
        relative = line[3:].split(" -> ")[-1]
        if any(part in relative for part in excluded):
            continue
        candidate = repo / relative
        if not candidate.is_file() or candidate.is_symlink():
            continue
        changed.append({"path": relative, "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest()})
    changed.sort(key=lambda value: value["path"])
    snapshot = {"head": head, "tree": tree, "parents": parents,
                "working_tree_clean": not bool(status.strip()), "changed_files": changed}
    source_hash = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    snapshot["source_sha256"] = source_hash
    base = None
    for reference in ("upstream/main", "origin/main", "refs/remotes/upstream/main"):
        result = subprocess.run(["git", "-C", str(repo), "merge-base", "HEAD", reference],
                                check=False, capture_output=True, text=True)
        if result.returncode == 0:
            base = result.stdout.strip()
            break
    snapshot["base_sha"] = base
    return snapshot


def _java_version(repo: Path) -> str:
    java_home = os.environ.get("ETCD_HBASE_JDK21_HOME")
    executable = Path(java_home) / "bin/java" if java_home else Path("java")
    result = subprocess.run([str(executable), "-version"], check=False,
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise ReportError("locked Java 21 version could not be recorded")
    return (result.stderr or result.stdout).strip().splitlines()[0]


def init_run(repo: Path, task_id: str, scope: str, artifact_root: Path) -> None:
    task = _load_task(task_id, repo / "testkit/cases/m5")
    if scope != "public":
        raise ReportError("M5 run scope must be public")
    if artifact_root.is_symlink() or not artifact_root.is_dir() or any(artifact_root.iterdir()):
        raise ReportError("M5 run requires a new empty artifact directory")
    if task_id == "J52":
        for entry in task["tests"]:
            report = (repo / entry["module"] / "target" / f"{entry['kind']}-reports"
                      / f"TEST-{entry['class']}.xml")
            if report.is_symlink():
                raise ReportError("J52 selected report must not be a symbolic link")
            report.unlink(missing_ok=True)
    registry_path = repo / "testkit/cases/m5" / f"{task_id}.json"
    profiles = task["maven_profiles"]
    run = {
        "schema": RUN_SCHEMA,
        "run_id": str(uuid.uuid4()),
        "task": task_id,
        "scope": scope,
        "status": "running",
        "started_at": _utc_now(),
        "finished_at": None,
        "source": _source_snapshot(repo),
        "protocol": PROTOCOL,
        "backend_adapters": task["backend_adapters"],
        "layouts": task["layouts"],
        "execution_profile": task["execution_profile"],
        "maven_profiles": profiles,
        "toolchain": _java_version(repo),
        "registry_sha256": hashlib.sha256(registry_path.read_bytes()).hexdigest(),
        "commands": [],
        "reports": [],
    }
    if task_id == "J51":
        run["unverified"] = [
            "operator inventory completeness, credential rotation and real HBase behavior",
            "target-environment clock, ACL and Lease acceptance",
            "real cross-process cache invalidation and target-environment acceptance",
        ]
    if task_id == "J52":
        run["unverified"] = [
            "cross-replica Compact owner routing and target-environment management authorization",
            "full-record physical=true completion beyond the KV-history-only RPC projection",
            "runtime result-window protection and all-record protection drain/completion",
            "target-environment acceptance and HFile space reclamation",
        ]
    _write_json(artifact_root / "run.json", run)


def record_command(run_file: Path, name: str, exit_code: int,
                   started_at: str, finished_at: str, stdout: str, stderr: str) -> None:
    if name not in COMMANDS | {"native-oracle"}:
        raise ReportError(f"unknown M5 verification command: {name}")
    run = _read_json(run_file)
    if run.get("schema") != RUN_SCHEMA or run.get("status") != "running":
        raise ReportError("M5 command cannot be attached to a non-running report")
    commands = run.get("commands")
    if not isinstance(commands, list) or any(item.get("name") == name for item in commands):
        raise ReportError(f"duplicate or invalid M5 command record: {name}")
    for relative in (stdout, stderr):
        if Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ReportError("M5 command log path must be relative to the artifact root")
    commands.append({"name": name, "exit_code": exit_code,
                     "started_at": started_at, "finished_at": finished_at,
                     "stdout": stdout, "stderr": stderr})
    run["commands"] = commands
    _write_json(run_file, run)


def _test_report(path: Path) -> tuple[int, int, int, int, set[str]]:
    if path.is_symlink() or not path.is_file():
        raise ReportError(f"registered Maven test report is missing: {path.name}")
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as error:
        raise ReportError(f"registered Maven test report is invalid: {path.name}") from error
    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
    if not suites or any(suite.tag != "testsuite" for suite in suites):
        raise ReportError(f"Maven report has no test suites: {path.name}")
    testcase_names: set[str] = set()
    tests = failures = errors = skipped = 0
    for suite in suites:
        try:
            tests += int(suite.attrib.get("tests", "0"))
            failures += int(suite.attrib.get("failures", "0"))
            errors += int(suite.attrib.get("errors", "0"))
            skipped += int(suite.attrib.get("skipped", "0"))
        except ValueError as error:
            raise ReportError(f"Maven test counters are invalid: {path.name}") from error
        testcase_names.update(case.attrib.get("name", "") for case in suite.findall("testcase"))
    if tests <= 0:
        raise ReportError(f"registered Maven report contains zero tests: {path.name}")
    if failures or errors or skipped:
        raise ReportError(f"registered Maven suite failed, errored, or skipped tests: {path.name}")
    return tests, failures, errors, skipped, testcase_names


def verify_reports(repo: Path, task_id: str, artifact_root: Path) -> None:
    task = _load_task(task_id, repo / "testkit/cases/m5")
    run_file = artifact_root / "run.json"
    run = _read_json(run_file)
    if (run.get("schema") != RUN_SCHEMA or run.get("task") != task_id
            or run.get("scope") != "public" or run.get("status") != "running"):
        raise ReportError("M5 test reports do not match a running task report")
    commands = run.get("commands")
    if not isinstance(commands, list):
        raise ReportError("M5 run has no verification command records")
    maven_commands = [entry for entry in commands if entry.get("name") == "maven-verify"]
    if len(maven_commands) != 1 or maven_commands[0].get("exit_code") != 0:
        raise ReportError("M5 test reports cannot be accepted without a successful Maven command")
    if task_id == "J52":
        _verify_j52_oracle(commands, artifact_root)
    reports_root = artifact_root / "reports"
    if reports_root.exists():
        raise ReportError("M5 reports directory already exists; refusing to reuse old report files")
    records = []
    for entry in task["tests"]:
        module = entry["module"]
        kind = entry["kind"]
        class_name = entry["class"]
        source = repo / module / "target" / f"{kind}-reports" / f"TEST-{class_name}.xml"
        tests, failures, errors, skipped, names = _test_report(source)
        required_methods = set(entry["methods"])
        if not required_methods.issubset(names):
            missing = sorted(required_methods - names)
            raise ReportError(f"registered Maven test methods did not run: {class_name}: {missing}")
        destination = reports_root / module / kind / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        records.append({
            "module": module,
            "kind": kind,
            "class": class_name,
            "tests": tests,
            "failures": failures,
            "errors": errors,
            "skipped": skipped,
            "required_methods": sorted(required_methods),
            "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        })
    run["reports"] = records
    run["test_counts"] = {
        "suites": len(records),
        "tests": sum(record["tests"] for record in records),
        "failures": sum(record["failures"] for record in records),
        "errors": sum(record["errors"] for record in records),
        "skipped": sum(record["skipped"] for record in records),
    }
    run["reports_verified_at"] = _utc_now()
    _write_json(run_file, run)


def finalize_run(run_file: Path, exit_code: int) -> None:
    run = _read_json(run_file)
    commands = run.get("commands")
    if not isinstance(commands, list):
        raise ReportError("M5 run command ledger is invalid")
    present = {entry.get("name") for entry in commands}
    failed = [entry.get("name") for entry in commands if entry.get("exit_code") != 0]
    required_commands = COMMANDS | ({"native-oracle"} if run.get("task") == "J52" else set())
    missing = sorted(required_commands - present)
    counts = run.get("test_counts")
    report_counts_valid = (
        isinstance(counts, dict)
        and all(type(counts.get(name)) is int
                for name in ("suites", "tests", "failures", "errors", "skipped"))
        and counts.get("suites", 0) > 0
        and counts.get("tests", 0) > 0
        and counts.get("failures", 0) == 0
        and counts.get("errors", 0) == 0
        and counts.get("skipped", 0) == 0
    )
    succeeded = (
        exit_code == 0 and not failed and not missing and bool(run.get("reports"))
        and report_counts_valid
    )
    run["status"] = "passed" if succeeded else "failed"
    run["process_exit_code"] = exit_code
    run["missing_commands"] = missing
    run["failed_commands"] = failed
    run["finished_at"] = _utc_now()
    _write_json(run_file, run)
    if exit_code == 0 and not succeeded:
        raise ReportError("M5 run cannot pass with a missing command or test report")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-registry", action="store_true")
    group.add_argument("--validate-task")
    group.add_argument("--validate-all", action="store_true")
    group.add_argument("--maven-selectors")
    group.add_argument("--init-run", action="store_true")
    group.add_argument("--record-command", action="store_true")
    group.add_argument("--verify-reports", action="store_true")
    group.add_argument("--finalize-run", action="store_true")
    parser.add_argument("--task")
    parser.add_argument("--scope")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--run-file", type=Path)
    parser.add_argument("--name")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--stdout")
    parser.add_argument("--stderr")
    arguments = parser.parse_args()

    try:
        if arguments.validate_registry:
            registered = _validate_registry()
            print(f"M5 registry valid: {len(registered)} executable task(s)")
        elif arguments.validate_task:
            task_id = _legal_task(arguments.validate_task)
            _load_task(task_id, REGISTRY)
            print(f"M5 task profile valid: {task_id}")
        elif arguments.validate_all:
            registered = _validate_registry()
            _validate_all(registered)
            print("All M5 task profiles are registered")
        elif arguments.maven_selectors:
            task = _load_task(_legal_task(arguments.maven_selectors), REGISTRY)
            surefire, failsafe = _maven_selectors(task)
            print(f"{surefire}\t{failsafe}")
        elif arguments.init_run:
            if not arguments.task or not arguments.scope or not arguments.artifact_root:
                raise ReportError("--init-run needs --task, --scope, and --artifact-root")
            init_run(REPO, _legal_task(arguments.task), arguments.scope,
                     arguments.artifact_root.resolve())
        elif arguments.record_command:
            required = (arguments.run_file, arguments.name, arguments.exit_code,
                        arguments.started_at, arguments.finished_at, arguments.stdout,
                        arguments.stderr)
            if any(value is None for value in required):
                raise ReportError("--record-command is missing required metadata")
            record_command(arguments.run_file, arguments.name, arguments.exit_code,
                           arguments.started_at, arguments.finished_at,
                           arguments.stdout, arguments.stderr)
        elif arguments.verify_reports:
            if not arguments.task or not arguments.artifact_root:
                raise ReportError("--verify-reports needs --task and --artifact-root")
            verify_reports(REPO, _legal_task(arguments.task), arguments.artifact_root.resolve())
        elif arguments.finalize_run:
            if arguments.run_file is None or arguments.exit_code is None:
                raise ReportError("--finalize-run needs --run-file and --exit-code")
            finalize_run(arguments.run_file, arguments.exit_code)
        return 0
    except ReportError as error:
        print(f"M5 evidence error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
