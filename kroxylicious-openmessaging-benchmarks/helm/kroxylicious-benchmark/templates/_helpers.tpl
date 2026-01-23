{{/*
Generate Kafka bootstrap servers list
*/}}
{{- define "kroxylicious-benchmark.kafkaBootstrapServers" -}}
{{- range $i, $e := until (int .Values.kafka.replicas) }}{{- if $i }},{{- end }}kafka-{{ $i }}.kafka:9092{{- end }}
{{- end }}

{{/*
Generate Kafka controller quorum voters list
*/}}
{{- define "kroxylicious-benchmark.kafkaControllerQuorum" -}}
{{- range $i, $e := until (int .Values.kafka.replicas) }}{{- if $i }},{{- end }}kafka-{{ $i }}@kafka-{{ $i }}.kafka:9093{{- end }}
{{- end }}
