# R2 isolated temporary credentials: prerequisite finding

Yes: the existing parent S3 access-key ID and secret can generate short-lived, prefix-scoped R2 credentials locally. No Cloudflare API token, credential-creation request, shared token rotation or revocation is needed. This removes the credential-minting question for an expiry-based isolated S3 qualification. It does not establish complete Q6 old-worker fencing.

## Exact supported path

Cloudflare documents local HS256 JWT signing with the parent secret. Required inputs: parent access-key ID and secret, account ID, exact R2 endpoint host, bucket, expiration interval, permission scope/actions and allowed object-key paths. Claims include `bucket`, `scope`, optional `actions`, `paths.prefixPaths`/`objectPaths`, `sub` account ID, `iss` parent key ID, `aud` endpoint host, `iat` and `exp`. Returned S3 credentials reuse the parent key ID, derive the temporary secret as SHA-256 hex of the signed JWT, and use base64(`jwt/` + JWT) as session token. Supply all three through maintained S3 credential fields. [Official complete example](https://developers.cloudflare.com/r2/examples/authenticate-r2-temp-credentials/).

Use one synthetic key prefix such as `__r2_data_catalog/docspec-qualification/<fresh-owner>/` with a trailing slash, not a bucket URI or broad catalog root. Prefix matching is literal key-start matching. One credential belongs to one bucket; permissions cannot exceed the parent. Local signing supports an explicit action list, allowing required reads/conditional PUT and any necessary multipart actions while excluding deletion and unrelated listing. Actual necessary actions must match the maintained client's transfer mode. [Temporary credential scoping](https://developers.cloudflare.com/r2/api/s3/temporary-credentials/).

The alternative REST endpoint `POST /accounts/{account_id}/r2/temp-access-credentials` requires bearer API-token authentication plus parent key ID, bucket, permission and TTL; its documented TTL maximum is seven days. That endpoint is unnecessary when only the parent S3 pair is available. [API contract](https://developers.cloudflare.com/api/resources/r2/subresources/temporary_credentials/methods/create/).

The parent S3 secret is a hash of the original API token; the pair is not the REST bearer token. Do not infer REST authorization from possession of the S3 secret. [R2 authentication](https://developers.cloudflare.com/r2/api/tokens/).

## Expiry and Q6 boundary

Documentation promises automatic expiry and permission attenuation. It describes immediate invalidation of derived credentials if the parent is revoked; doing that would affect shared SpicyRegs access and is excluded. I found no documented individual-child revocation endpoint, expiry clock-skew bound, or guarantee that expiry cancels an already authenticated PUT/GET, drains multipart work, or prevents an accepted request from completing afterward in the reviewed official reference/example/API pages.

Inference: a worker receiving only the derived triple cannot renew the parent-signed JWT because it lacks the parent secret. The trusted controller still can mint another credential, so qualification must withhold the parent secret/API bearer and disable ambient credential fallback in the old worker. Expiry is a future-request authentication boundary, not demonstrated cancellation of an in-flight write. A rejected post-expiry canary is useful evidence but does not prove in-flight draining. Multipart upload artifacts remain a separate retained cleanup concern.

Basin Catalog is a separate authority: its documented engine authentication requires an API token with catalog and storage permissions, and it can vend SigV4 storage credentials. The reviewed docs do not demonstrate using the locally signed S3 triple as a REST catalog bearer or prefix-fencing catalog commits. Leaving the existing catalog bearer accessible to an old worker therefore does not prove that worker fenced by S3 expiry. [Basin authentication and vended credentials](https://developers.cloudflare.com/basin-catalog/manage-catalogs/#authenticate-your-iceberg-engine).

## Decision

No new user credential/API-token request is needed solely to mint isolated nonrenewable-to-the-worker temporary S3 credentials. Existing authorization covers the synthetic bucket prefix. A safe expiry-based S3-only test can be prepared with trusted local signing, no parent mutation, a fixed child expiration, out-of-prefix refusal and post-expiry refusal observations.

Keep the full Q6 conclusion pending exact catalog-authority fencing and in-flight request semantics/drain. If immediate revocation of one complete remote Engine's storage and catalog authority is the required test, the existing pair alone has not been shown sufficient; request that precise missing isolated capability rather than asking generally for a Cloudflare API token. No provider calls, secret reads, builds, repository edits or credential changes were made for this investigation.
