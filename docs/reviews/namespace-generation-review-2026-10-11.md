# Namespace-generation seam: static review

Reviewed `/tmp/docspec-namespace-generation.patch`, SHA256
`70b2e1f1eb65ba9e219a6c4e4ebf70f91dec8367b5f365fd7e9908526025d1f9`.
Static verdict: **approve this routing seam**. No build/provider execution performed.
This is not approval or qualification of remote removal/restore.

`storage/remote/namespaces.rs` fixes one canonical UUID generation at construction.
Logical flat identifiers map to that physical root; foreign generation identifiers
refuse. Enumeration asks the maintained catalog for the selected root rather than
sweeping other generations. Default absent configuration retains existing routing.
`RemoteCatalog::update_table` checks physical identity and forwards the original
native commit unchanged: a stale commit is never rewritten to the new registration.

`catalog.rs::View` now separates logical record identity from the native Table.
Admission, protection/retirement checks, captured candidate identities and output
namespace selection use the logical identity. The exact commit precheck retains
`view.table.identifier()`; native source-head reloads also retain native identity.
Thus current protections and saved records are not rebound by routing configuration.

The maintained local fixture detaches the old registration, registers its exact
metadata URI/UUID in a new physical generation, proves a successful new commit,
and requires the detached old commit/load to fail. It preserves old snapshot reads
and logical identity while proving the native identifiers differ. This respects
the observed provider duplicate-UUID restriction; it does not simulate concurrent
registrations or prove provider fencing/authentication behavior.

No actionable static finding. Remaining connected maintenance must enforce owned
configured inventory, quiescent authority and permanent unavailable object keys;
namespace routing alone does not exclude old object writers/readers or stop an
already accepted remote effect. No backup copying is needed by this seam.
