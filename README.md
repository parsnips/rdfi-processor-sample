# Twisp RDFI Processor Example

This repository contains a small Go demonstration program for Twisp RDFI ACH processing. It runs a local ACH processor webhook, uploads RDFI ACH files into a local Twisp, processes them serially, prints every GraphQL request/response, prints every webhook request/response, and renders generated return/NOC files.

## Prerequisites

Start Twisp local:

```sh
docker run -p 3000:3000 -p 8080:8080 -p 8081:8081 public.ecr.aws/twisp/local:latest
```

Run the demo:

```sh
go run .
```

The default GraphQL endpoint is `http://localhost:8080/financial/v1/graphql`. The default webhook listener is `0.0.0.0:8099`, and the endpoint registered in Twisp is `http://host.docker.internal:8099/rdfi`, which lets the Docker container call back to the host on Docker Desktop.

## Running Scenarios

Run every scenario:

```sh
go run .
```

Run one scenario or a pattern-matched subset:

```sh
go run . -scenario ppd-debit-return-create
go run . -scenario 'iat|noc'
go run . -scenario pending
```

Useful flags:

```sh
go run . \
  -graphql http://localhost:8080/financial/v1/graphql \
  -listen 0.0.0.0:8099 \
  -webhook-url http://host.docker.internal:8099/rdfi \
  -twisp-account-id 000000000000
```

If your Twisp deployment requires an authorization header, pass `-authorization 'Bearer ...'` or set `AUTHORIZATION`.

## Covered Cases

The demo includes:

- `ppd-debit-accepted`: PPD debit accepted and settled.
- `ppd-credit-accepted`: PPD credit accepted and settled.
- `iat-debit-accepted`: IAT debit accepted and settled.
- `iat-debit-returned`: IAT debit returned at create time with addenda99.
- `ppd-debit-retry`: first webhook returns `RETRY`, then settles on retry.
- `ppd-debit-pending`: create returns `PENDING`; the program prints manual workflow invocations and then executes a return.
- `ppd-debit-return-create`: PPD debit returned at create time with customer addenda99.
- `ppd-debit-return-settle`: PPD debit returned at settle time with customer addenda99.
- `ppd-debit-unknown-account`: processor returns an unknown account id, causing Twisp to use suspense/default return behavior.
- `ppd-debit-noc`: settle response includes addenda98 and generates a NOC file.

## How Routing Works

Each scenario is selected by the DDA/account number in the ACH entry detail. The program derives input ACH files from Twisp's PPD debit, PPD credit, and IAT debit test fixtures, replacing only the DDA field with a deterministic scenario value.

The webhook handler reads `entryDetail.dfiAccountNumber` from the JSON payload and returns the configured action for that DDA:

- `SETTLE` with `accountId`
- `RETURN` with `addenda99`
- `RETRY`
- `PENDING`
- optional `addenda98` on settlement for NOC generation

## Output Structure

For each scenario the program prints:

1. scenario name and description
2. create upload GraphQL request/response
3. file upload request/response
4. process file GraphQL request/response
5. webhook request/response pairs
6. file status polling result
7. manual workflow invocation details for pending entries
8. return/NOC generate request/response when applicable
9. download link response
10. rendered ACH return/NOC file content

Generated file keys are named `rdfi-example-<run-id>-<scenario>...` in Twisp local's file store. They are intentionally flat because Twisp local's file server writes upload keys directly under its local file directory.

## Manual Pending Touch Point

For `ppd-debit-pending`, the webhook returns:

```json
{
  "action": "PENDING",
  "accountId": "6c6affb0-5cf5-402b-8d84-01bfc1624a2c"
}
```

The program prints the `workflow.execute` inputs needed to settle or return the pending entry. It then executes the return path automatically so the example can also demonstrate return file generation.

## Notes

The setup step creates idempotent ledger accounts with stable IDs and creates a fresh endpoint/configuration for each run. This makes repeated runs safe without clearing the local Twisp container.
