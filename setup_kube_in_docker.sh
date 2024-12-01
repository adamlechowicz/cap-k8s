#!/bin/bash

# Copy the kube-scheduler manifest file to a new location
cp /etc/kubernetes/manifests/kube-scheduler.yaml /etc/kubernetes/kube-scheduler.yaml

# Copy the local file sched-cc.yaml to /etc/kubernetes
cp sched-cc.yaml /etc/kubernetes/sched-cc.yaml

# apply the all-in-one kube-scheduler manifest file
kubectl apply -f /kube-scheduler-manifests/all-in-one.yaml

# apply the other files in the kube-scheduler-manifests directory
kubectl apply -f /kube-scheduler-manifests/networktopology.diktyo.x-k8s.io_networktopologies.yaml
kubectl apply -f /kube-scheduler-manifests/topology.node.k8s.io_noderesourcetopologies.yaml

# Copy the local file kube-scheduler.yaml to /etc/kubernetes/manifests/kube-scheduler.yaml
cp kube-scheduler.yaml /etc/kubernetes/manifests/kube-scheduler.yaml

# restart the kubelet service
systemctl restart kubelet.service

# wait a few seconds
sleep 5

# check to see if things are running
kubectl get pod -n kube-system | grep kube-scheduler

# check the image
kubectl get pods -l component=kube-scheduler -n kube-system -o=jsonpath="{.items[0].spec.containers[0].image}{'\n'}"


