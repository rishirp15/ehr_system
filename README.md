# Distributed EHR System

A distributed Electronic Health Record system implementing advanced distributed algorithms for scalability, consistency, and reliability.

## Features

### Distributed Algorithms
- **RPC/RMI Communication**: XML-RPC for inter-node communication
- **Clock Synchronization**: Berkeley algorithm for consistent timestamps
- **Mutual Exclusion**: Maekawa's algorithm for concurrent access
- **Deadlock Detection**: Wait-for graph based detection and resolution
- **Quorum Consensus**: For data consistency across nodes
- **Load Balancing**: Enhanced with health monitoring

### System Architecture
- **API Gateway**: Central entry point with load balancing
- **Application Nodes**: 3 nodes for request processing
- **Data Nodes**: 3 nodes with data replication
- **Redis Cache**: For frequent queries and performance
- **Dockerized**: Complete containerization

## Quick Start

1. **Clone and setup**:
```bash
git clone <repository>
cd distributed-ehr-system