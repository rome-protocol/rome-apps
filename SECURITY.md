# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately** through GitHub's private
vulnerability reporting:

1. Open the **Security** tab of this repository.
2. Select **Report a vulnerability**.
3. Describe the issue, the affected component and version or commit, and steps to
   reproduce. Include a proof of concept if you have one.

**Do not open a public issue, pull request or discussion for a security problem.** Public
disclosure before a fix is available puts users at risk.

We will acknowledge your report, investigate, and keep you informed of progress. Please give
us reasonable time to address the issue before any public disclosure.

## Scope

This policy covers the services and tools in this repository:

- `proxy` (Ethereum JSON-RPC endpoint)
- `hercules` (block indexer and its admin RPC)
- `cli`
- `rome-via-api`, `rome-via-sync`, `rome-via-enrich`, `rome-via-classify`
- `rome-audit`
- `cardo-service`
- `dammv1-pool-constructor`
- the Docker build in `docker/`

Issues in sibling repositories (for example `rome-sdk` or `rome-evm`) should be reported
through the Security tab of the repository concerned.

Out of scope: findings that depend on a deployment exposing interfaces that the
documentation says must stay private (such as the Hercules admin RPC, health ports or the
metrics sidecar), denial of service through raw traffic volume against an endpoint without
a reverse proxy, and issues in third-party dependencies that are already publicly known.

## Rewards

There is no bug bounty program for this repository, and submitting a report does not
create any entitlement to a reward.
