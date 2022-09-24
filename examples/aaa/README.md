# Data Prepper Minimum Viable Examples

There are 3 examples in this folder. They all have the same dataflow:

OTEL emitter -> OTEL collector -> Data Prepper -> OpenSearch/OpenSearch Dashboard

## Released
- All components are run in containers and in the same container network "my_network".
- Data Prepper uses the latest image, which has version 1.5.1 at this time

## Snapshot
- All components are run in containers and in the same container network "my_network".
- Data Prepper image is built from source, 2.0.0-SNAPSHOT

## Snapshot2
- All components except Data Prepper are run in containers and in the same container network "my_network".
- Data Prepper is built from source, 2.0.0-SNAPSHOT, and run directly on host machine. This means that there will be connections between components inside and outside the container network
  - OTEL collector to Data Prepper: this is a link from inside container to a port on host machine. We use `host.docker.internal` to refer to host machine in the container. Note that in `docker-compose.yml`, those lines are added under `otel-collector`:
  ```yaml
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ```
  
  - Data Prepper to OpenSearch: this is a link from host machine to a port on the container network. This is normal practice - publish and map port on the container to port on host machine and just connect to the port on localhost


