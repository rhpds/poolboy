{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "poolboy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "poolboy.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "poolboy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "poolboy.labels" -}}
helm.sh/chart: {{ include "poolboy.chart" . }}
{{ include "poolboy.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "poolboy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "poolboy.name" . }}
{{-   if (ne .Release.Name "RELEASE-NAME") }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{-   end -}}
{{- end -}}

{{- define "poolboy.redisSelectorLabels" -}}
app.kubernetes.io/name: {{ include "poolboy.name" . }}-redis
{{-   if (ne .Release.Name "RELEASE-NAME") }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{-   end -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "poolboy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "poolboy.name" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Create the name of the namespace to use
*/}}
{{- define "poolboy.namespaceName" -}}
{{- if .Values.namespace.create -}}
    {{ default (include "poolboy.name" .) .Values.namespace.name }}
{{- else -}}
    {{ default (include "poolboy.name" .) .Values.namespace.name }}
{{- end -}}
{{- end -}}


{{/*
Create the operator domain to use
*/}}
{{- define "poolboy.operatorDomain" -}}
{{- if .Values.operatorDomain.generate -}}
    {{ default (printf "%s.gpte.redhat.com" (include "poolboy.name" .)) .Values.operatorDomain.name }}
{{- else -}}
    {{ default "default" .Values.operatorDomain.name }}
{{- end -}}
{{- end -}}

{{/*
Define the image to deploy
*/}}
{{- define "poolboy.image" -}}
  {{- if eq .Values.image.tagOverride "-" -}}
    {{- .Values.image.repository -}}
  {{- else if .Values.image.tagOverride -}}
    {{- printf "%s:%s" .Values.image.repository .Values.image.tagOverride -}}
  {{- else -}}
    {{- printf "%s:v%s" .Values.image.repository .Chart.AppVersion -}}
  {{- end -}}
{{- end -}}

{{/*
Determine if operator is running in standalone mode.
Backward compatibility mapping:
  - 'all-in-one' -> standalone (true)
  - 'standalone' -> standalone (true)
  - 'manager', 'resource-handler', 'resource-watch' -> distributed (false)
  - 'distributed' -> distributed (false)
  - any other value -> distributed (false)
*/}}
{{- define "poolboy.isStandalone" -}}
{{- $mode := .Values.operatorMode | default "distributed" -}}
{{- if or (eq $mode "standalone") (eq $mode "all-in-one") -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}
