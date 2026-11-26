package io.etcdhbase.server.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.spi.identity.AuthenticationScope;
import io.etcdhbase.spi.identity.AuthorizedBackendIdentity;
import java.util.HexFormat;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SubjectIdentityDigestsTest {
  @Test
  void verifiedSameFamilyAcrossGroupsMatchesButProviderAndDomainDoNot() {
    var a = identity("provider", "alice", "group-a");
    var b = identity("provider", "alice", "group-b");
    var family = SubjectIdentityDigests.family("trust", "realm-a", a);
    assertEquals(family, SubjectIdentityDigests.family("trust", "realm-a", b));
    assertNotEquals(family, SubjectIdentityDigests.family("trust", "realm-b", b));
    assertNotEquals(family, SubjectIdentityDigests.family("other", "realm-a", b));
    assertNotEquals(family, SubjectIdentityDigests.family("trust", "realm-a",
        identity("other-provider", "alice", "group-a")));
    assertNotEquals(family, SubjectIdentityDigests.family("trust", "realm-a",
        identity("provider", "bob", "group-a")));
    assertThrows(IllegalArgumentException.class, () -> SubjectIdentityDigests.family("trust", null, a));
  }

  @Test
  void scopedDigestBindsTheCredentialFamilyAndScopeWithAnExplicitVersion() {
    var scope = new AuthenticationScope("group-a", "profile");
    var digest = SubjectIdentityDigests.scoped("trust", "provider", "realm-a", scope, "alice");
    assertEquals("dd7dfae4a6e63205039ea68c35933b919e3d3e54252a76e6d5465497b11e732e",
        HexFormat.of().formatHex(digest.bytes()));
    assertNotEquals("5da702f82268e51fd80c1043ffbf85681fb59fb0c1b675dd4bba8823451097d5",
        HexFormat.of().formatHex(digest.bytes()));
    assertNotEquals(digest, SubjectIdentityDigests.scoped("trust", "provider", "realm-b", scope, "alice"));
    assertNotEquals(digest, SubjectIdentityDigests.scoped("other", "provider", "realm-a", scope, "alice"));
    assertNotEquals(digest, SubjectIdentityDigests.scoped("trust", "other", "realm-a", scope, "alice"));
    assertNotEquals(digest, SubjectIdentityDigests.scoped("trust", "provider", "realm-a", scope, "bob"));
    assertNotEquals(digest, SubjectIdentityDigests.scoped("trust", "provider", "realm-a",
        new AuthenticationScope("group-b", "profile"), "alice"));
    assertNotEquals(digest, SubjectIdentityDigests.scoped("trust", "provider", "realm-a",
        new AuthenticationScope("group-a", "other-profile"), "alice"));
    assertThrows(IllegalArgumentException.class,
        () -> SubjectIdentityDigests.scoped("trust", "provider", null, scope, "alice"));
  }

  private static AuthorizedBackendIdentity identity(String provider, String subject, String group) {
    return new AuthorizedBackendIdentity(provider, subject, "opaque-reference",
        new AuthenticationScope(group, "profile"), Set.of(group), 1);
  }
}
