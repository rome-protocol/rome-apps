#!/bin/bash

if [ -z "${CHAIN_ID}" ]; then
  echo "CHAIN_ID is not defined"
  exit 1
fi;

if [ -z "${PROGRAM_ID}" ]; then
  echo "PROGRAM_ID is not defined"
  exit 1
fi;

if [ -z "${SOLANA_RPC}" ]; then
  echo "SOLANA_RPC is not defined"
  exit 1
fi;

if [ -z "${COMMAND}" ]; then
  echo "COMMAND is not defined"
  exit 1
fi;

if [[ "$COMMAND" == "reg-rollup" ]]; then
  if [ -z "${REGISTRY_AUTHORITY}" ]; then
    echo "REGISTRY_AUTHORITY is not defined"
    exit 1
  fi;

  if [ -z "${IS_SINGLE_STATE}" ]; then
    echo "IS_SINGLE_STATE is not defined"
    exit 1
  fi;

  if [ -z "${MINT}" ]; then
    ./cli --program-id "${PROGRAM_ID}" --chain-id "${CHAIN_ID}" --url "${SOLANA_RPC}" --keypair "${REGISTRY_AUTHORITY}" "${COMMAND}" "${IS_SINGLE_STATE}"
  else
    ./cli --program-id "${PROGRAM_ID}" --chain-id "${CHAIN_ID}" --url "${SOLANA_RPC}" --keypair "${REGISTRY_AUTHORITY}" "${COMMAND}" "${IS_SINGLE_STATE}" "${MINT}"
  fi;

elif [[ "$COMMAND" == "deposit" ]]; then
  if [ -z "${ADDRESS}" ]; then
    echo "ADDRESS is not defined"
    exit 1
  fi;

  if [ -z "${BALANCE}" ]; then
    echo "BALANCE is not defined"
    exit 1
  fi;

  if [ -z "${KEYPAIR}" ]; then
    echo "KEYPAIR is not defined"
    exit 1
  fi;

  ./cli --program-id "${PROGRAM_ID}" --chain-id "${CHAIN_ID}" --url "${SOLANA_RPC}" --keypair "${KEYPAIR}" "${COMMAND}" "${ADDRESS}" "${BALANCE}"
else
  echo "Unknown cli command $COMMAND"
  exit 1
fi;

