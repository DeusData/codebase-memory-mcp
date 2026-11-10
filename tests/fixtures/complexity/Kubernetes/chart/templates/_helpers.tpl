{{- define "cx.image" -}}
{{- $reg := .registry -}}
{{- printf "%s/%s:%s" $reg .repository .tag -}}
{{- end -}}
