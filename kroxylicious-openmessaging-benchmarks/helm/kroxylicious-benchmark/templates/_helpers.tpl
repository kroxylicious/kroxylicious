{{/*
Copyright Kroxylicious Authors.

Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
*/}}

{{/*
Generate bootstrap servers - routes to Kroxylicious proxy if enabled, otherwise direct to Kafka
*/}}
{{- define "kroxylicious-benchmark.kafkaBootstrapServers" -}}
{{- if .Values.kroxylicious.enabled -}}
kroxylicious-service:9092
{{- else -}}
kafka-kafka-bootstrap:9092
{{- end -}}
{{- end }}

{{/*
Generate OMB worker list as comma-separated URLs
*/}}
{{- define "kroxylicious-benchmark.workerList" -}}
{{- $workers := list -}}
{{- range $i := until (int .Values.omb.workerReplicas) -}}
{{- $workers = append $workers (printf "http://omb-worker-%d.omb-worker.%s.svc:8080" $i $.Release.Namespace) -}}
{{- end -}}
{{- join "," $workers -}}
{{- end }}
