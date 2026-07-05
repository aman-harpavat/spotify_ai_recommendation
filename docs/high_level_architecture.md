# High-Level Architecture Diagram

This document shows the deployed high-level architecture of the AI Product Discovery Copilot, including the public GPT, hosted backend, storage, and external source dependencies.

## 1. System Overview

```mermaid
flowchart LR
    U[User]

    subgraph OAI[OpenAI]
        GPT[Custom GPT / Agent 0]
    end

    subgraph GH[GitHub]
        REPO[GitHub Repository]
        PAGES[GitHub Pages<br/>privacy-policy.html]
    end

    subgraph RWY[Railway]
        API[FastAPI Backend]
        JOB[Async Run Manager]
        VOL[Persistent Volume<br/>/app/data]
    end

    subgraph SRC[Public Feedback Sources]
        GP[Google Play Reviews]
        AS[Apple App Store Reviews]
        RD[Reddit Public Discussions]
    end

    U --> GPT
    GPT -->|Privacy policy URL| PAGES
    GPT -->|Action calls| API
    API --> JOB
    JOB --> GP
    JOB --> AS
    JOB --> RD
    JOB --> VOL
    API --> VOL
    REPO -->|Deploy source| API
```

## 2. GPT-to-Backend Interaction

```mermaid
sequenceDiagram
    participant User
    participant GPT as Custom GPT
    participant API as Railway FastAPI Backend
    participant Vol as Persistent Volume

    User->>GPT: Natural-language research request
    GPT->>User: Clarifies missing fields
    User->>GPT: Confirms locked brief / go ahead
    GPT->>API: POST /analyze-feedback/start
    API->>Vol: Create run folder + status file
    API-->>GPT: run_id + status + ETA
    GPT->>API: GET /runs/{run_id}/status
    API->>Vol: Read run status
    API-->>GPT: running or completed
    GPT->>API: GET /runs/{run_id}/manifest
    GPT->>API: GET artifact files
    API->>Vol: Read artifacts
    API-->>GPT: compact payload + evidence artifacts
    GPT-->>User: Final PM research report
```

## 3. Backend Evidence Pipeline

```mermaid
flowchart TD
    A[Locked Brief Received] --> B[Async Run Created]
    B --> C[Collect Google Play Reviews]
    B --> D[Collect App Store Reviews]
    B --> E[Collect Reddit Public Discussions]
    C --> F[Merge Raw Feedback]
    D --> F
    E --> F
    F --> G[Cleaning and Normalization]
    G --> H[Time Window Filtering]
    H --> I[Relevance Filtering]
    I --> J[Deduplication]
    J --> K[Clustering]
    K --> L[Metrics and Charts]
    L --> M[Artifact Generation]
    M --> N[Compact GPT Payload]
    M --> O[Full Evidence Artifacts]
    N --> P[Status = completed or partial_success]
    O --> P
```

## 4. Stored Data

```mermaid
flowchart LR
    subgraph VOL[Railway Persistent Volume]
        RUNS[data/runs/{run_id}/]
        STATUS[_run_status.json]
        LOG[run.log]
        RAW[all_feedback_raw.csv]
        CLEAN[all_feedback_clean.csv]
        CLUSTERS[all_clusters*.json / csv]
        ARTIFACTS[charts_data.json<br/>research_question_coverage.json<br/>segment_evidence.json<br/>quality_diagnostics.json<br/>etc.]
    end

    RUNS --> STATUS
    RUNS --> LOG
    RUNS --> RAW
    RUNS --> CLEAN
    RUNS --> CLUSTERS
    RUNS --> ARTIFACTS
```

## 5. Deployment View

```mermaid
flowchart TB
    DEV[Local Repo<br/>product_discovery_copilot]
    GIT[GitHub<br/>aman-harpavat/product_discovery_copilot]
    RAILWAY[Railway Service<br/>product_discovery_copilot]
    DOMAIN[Railway Public Domain<br/>productdiscoverycopilot-production.up.railway.app]
    GPT[Public GPT<br/>Anyone with the link]
    POLICY[GitHub Pages Privacy Policy]

    DEV --> GIT
    GIT --> RAILWAY
    RAILWAY --> DOMAIN
    GPT --> DOMAIN
    GPT --> POLICY
```

## 6. Notes

- The GPT is the PM reasoning layer.
- The backend is the evidence preparation layer.
- Public feedback is collected live from supported sources.
- Run artifacts are stored on the Railway volume and auto-deleted after the retention window.
- GitHub Pages hosts the public privacy policy required for GPT Actions.
