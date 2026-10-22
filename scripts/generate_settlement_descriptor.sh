#!/bin/sh
# Regenerate the CEX16 worker's protobuf descriptor after editing its contract.
# Requires protoc, with no Go code-generation plugin needed.
set -eu
cd "$(dirname "$0")/.."
protoc \
  --proto_path=internal/settlementworker/contract \
  --descriptor_set_out=internal/settlementworker/contract/settlement_instruction.pbdesc \
  settlement_instruction.proto
