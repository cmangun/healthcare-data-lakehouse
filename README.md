# Healthcare Data Lakehouse

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![HIPAA Compliant](https://img.shields.io/badge/HIPAA-Compliant-green.svg)](#compliance)

**Production data lakehouse with medallion architecture for healthcare data with full lineage tracking.**

## Business Impact

- **Full data provenance** from raw ingestion to ML-ready features
- **100% audit coverage** for FDA and HIPAA compliance
- **Automated quality checks** at every transformation stage
- **Impact analysis** before schema changes

## Architecture (Medallion)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
│   Landing   │───▶│   Bronze    │───▶│   Silver    │───▶│    Gold      │
│   (Raw)     │    │ (Immutable) │    │ (Cleansed)  │    │ (Aggregated) │
└─────────────┘    └─────────────┘    └─────────────┘    └──────────────┘
                                                                 │
                                                                 ▼
                                                         ┌──────────────┐
                                                         │   Platinum   │
                                                         │ (ML Features)│
                                                         └──────────────┘
```

## Key Features

- **Medallion Architecture**: Landing → Bronze → Silver → Gold → Platinum
- **Full Data Lineage**: Track every transformation
- **Column-Level Lineage**: Know exactly where each column comes from
- **Quality Gates**: Automated validation at each stage
- **Impact Analysis**: Understand downstream effects before changes

## Quick Start

```bash
pip install -e ".[dev]"
pytest
```

## Author

**Christopher Mangun** - [LinkedIn](https://linkedin.com/in/cmangun)
