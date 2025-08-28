#!/bin/sh
KOPF_OPTIONS="${KOPF_OPTIONS:---log-format=json}"

# Restrict watch to operator namespace.
KOPF_NAMESPACED="${KOPF_NAMESPACED:-false}"

# Do not attempt to coordinate with other kopf operators.
KOPF_STANDALONE=true
