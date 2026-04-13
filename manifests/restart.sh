#!/bin/bash
kubectl delete deployments --all
kubectl delete daemonsets --all
kubectl delete cm --all

kubectl apply -f custom-metric-config.yaml
kubectl apply -f emsconfig.yaml
#kubectl apply -f myconfig.yaml
kubectl apply -f ems+netdata-k3s_parametric.yaml
kubectl apply -f stomp-listener.yaml
kubectl apply -f python_manifest.yaml
